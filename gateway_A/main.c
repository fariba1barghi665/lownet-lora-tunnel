#include <stdio.h>
#include <string.h>
#include <esp_wifi.h>
#include <esp_now.h>
#include <nvs_flash.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <esp_log.h>

#include "lownet.h"
#include "lownet_packet.h" 
#include "net_time.h"       
#include "lora_driver.h"    // lora_init(), lora_send(), lora_recv()

static const char* TAG = "GW-A";

// Queues
static QueueHandle_t q_now_rx;
static QueueHandle_t q_now_tx;
static QueueHandle_t q_lora_tx;

static const uint8_t LOCAL_SUBNET = 1;

// TODO: put REAL Node-A MAC here (peer in subnet A to downlink packets)
static uint8_t NODE_A_MAC[6] = { 0x48,0xE7,0x29,0x99,0x22,0x14 };

static inline uint8_t dst_subnet(uint8_t node_id) { return (node_id >> 4) & 0x0F; }

static void espnow_recv_cb(const esp_now_recv_info_t *info, const uint8_t *data, int len) {
    (void)info;
    if (len == sizeof(lownet_packet_t)) {
        lownet_packet_t pkt; memcpy(&pkt, data, len);
        xQueueSend(q_now_rx, &pkt, 0);
    }
}

// ESP-NOW RX → decide local delivery or forward to LoRa
static void now_rx_task(void* arg) {
    (void)arg;
    lownet_packet_t pkt;
    while (1) {
        if (xQueueReceive(q_now_rx, &pkt, portMAX_DELAY)) {
            uint64_t now_us = net_now_us();
            double delay_ms = pkt.timestamp_ns ?
                ((double)((int64_t)now_us - (int64_t)pkt.timestamp_ns) / 1000.0) : 0.0;
            if (delay_ms < 0) delay_ms = 0.0;

            ESP_LOGI("NOW-RX",
                     "ESP-NOW RX | src=0x%02X dst=0x%02X | latency=%.3f ms | hops=%u",
                     pkt.src_node_id, pkt.dst_node_id, delay_ms, pkt.hop_count);

            // Control-plane: propagate TIME frames to LoRa side so GW-B synchronizes as well
            if (lnet_payload_proto(&pkt) == LOWNET_PROTOCOL_TIME) {
                xQueueSend(q_lora_tx, &pkt, 0);
                continue;
            }

            const uint8_t dsub = dst_subnet(pkt.dst_node_id);
            if (dsub == LOCAL_SUBNET) {
                // Local subnet delivery (nodes in subnet A usually hear each other directly)
            } else {
                // Forward to LoRa → GW-B
                lownet_packet_t f = pkt;
                f.timestamp_ns = net_now_us();   // timestamp in microseconds
                f.hop_count++;
                f.gateway_recv_time = now_us;    // gateway receive time in microseconds

                ESP_LOGI("FWD",
                         "FORWARD LORA | src=0x%02X dst=0x%02X | subnet %u->%u | hops=%u",
                         pkt.src_node_id, pkt.dst_node_id, LOCAL_SUBNET, dsub, f.hop_count);

                xQueueSend(q_lora_tx, &f, 0);
            }
        }
    }
}

// Downlink to Node-A (e.g., PONG coming back from subnet B)
static void now_tx_task(void* arg) {
    (void)arg;
    lownet_packet_t pkt;
    while (1) {
        if (xQueueReceive(q_now_tx, &pkt, portMAX_DELAY)) {
            esp_now_send(NODE_A_MAC, (uint8_t*)&pkt, sizeof(pkt));
            vTaskDelay(pdMS_TO_TICKS(5));
        }
    }
}

// LoRa RX → deliver to subnet A if applicable
static void lora_rx_task(void* arg)
{
    (void)arg;
    lownet_packet_t pkt;

    while (1) {

        // short timeout to avoid blocking TX
        bool got = lora_recv(&pkt, 500);

        if (got) {

            uint64_t now_us = net_now_us();
            double delay_ms = pkt.timestamp_ns ?
                ((double)((int64_t)now_us - (int64_t)pkt.timestamp_ns) / 1000.0)
                : 0.0;

            ESP_LOGI("LORA-RX",
                     "LORA RX | src=0x%02X dst=0x%02X | latency=%.3f ms | hops=%u",
                     pkt.src_node_id,
                     pkt.dst_node_id,
                     delay_ms,
                     pkt.hop_count);

            // Forward to Node-A if destination is in subnet A
            if (((pkt.dst_node_id >> 4) & 0x0F) == LOCAL_SUBNET) {

                ESP_LOGI("LORA-RX",
                         "Forwarding to ESP-NOW (Node-A) | src=0x%02X dst=0x%02X",
                         pkt.src_node_id,
                         pkt.dst_node_id);

                xQueueSend(q_now_tx, &pkt, 0);
            }
        }

        // Let TX get the mutex (critical!)
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}


static void lora_tx_task(void* arg) {
    (void)arg;
    lownet_packet_t pkt;
    
    while (1) {
        if (xQueueReceive(q_lora_tx, &pkt, portMAX_DELAY)) {
            // Log packet type for debugging
            const char* pkt_type = "UNKNOWN";
            if (lnet_payload_proto(&pkt) == LOWNET_PROTOCOL_PING) {
                pkt_type = "PING";
            } else if (lnet_payload_proto(&pkt) == LOWNET_PROTOCOL_TIME) {
                pkt_type = "TIME";
            }
            
            ESP_LOGI("LORA-TX", "Sending %s packet | src=0x%02X dst=0x%02X", 
                     pkt_type, pkt.src_node_id, pkt.dst_node_id);
            
            // Send and check result
            bool sent = lora_send(&pkt);
            if (sent) {
                ESP_LOGI("LORA-TX", "Packet sent successfully");
            } else {
                ESP_LOGE("LORA-TX", "Failed to send packet!");
            }
            
            vTaskDelay(pdMS_TO_TICKS(100));  // Small delay between transmissions
        }
    }
}

// GW-A acts as the time master for the entire tunnel

static void time_sync_task(void* arg)
{
    const uint8_t GW_B = 0x21;  // Only Gateway-B receives TIME frames

    while (1) {
        uint64_t now_us = net_now_us();

        lownet_time_t t = {
            .seconds = (uint32_t)(now_us / 1000000ULL),
            .parts   = (uint8_t)(((now_us % 1000000ULL) * 256ULL) / 1000000ULL)
        };

        lownet_packet_t p = {0};
        p.src_node_id = 0x11;   // Gateway-A
        p.dst_node_id = GW_B;   // Gateway-B ONLY
        p.timestamp_ns = now_us;

        lnet_set_proto(&p, LOWNET_PROTOCOL_TIME);
        lnet_set_len(&p, sizeof(lownet_time_t));
        memcpy(&p.payload[2], &t, sizeof(t));

        ESP_LOGI("TIME", "Sending TIME to GW-B...");

        xQueueSend(q_lora_tx, &p, 0);

        vTaskDelay(pdMS_TO_TICKS(2000)); // every 2 seconds
    }
}


void app_main(void) {
    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_netif_init());

    // Unified time base
    net_time_init();

    // WiFi + ESP-NOW
    wifi_init_config_t wc = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&wc));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    ESP_ERROR_CHECK(esp_wifi_set_channel(1, WIFI_SECOND_CHAN_NONE));

    // Queues
    q_now_rx  = xQueueCreate(32, sizeof(lownet_packet_t));
    q_now_tx  = xQueueCreate(32, sizeof(lownet_packet_t));
    q_lora_tx = xQueueCreate(32, sizeof(lownet_packet_t));

    // ESP-NOW
    ESP_ERROR_CHECK(esp_now_init());
    ESP_ERROR_CHECK(esp_now_register_recv_cb(espnow_recv_cb));
    esp_now_peer_info_t peer = {0};
    memcpy(peer.peer_addr, NODE_A_MAC, 6);
    peer.channel = 1;
    peer.ifidx = ESP_IF_WIFI_STA;
    peer.encrypt = false;
    ESP_ERROR_CHECK(esp_now_add_peer(&peer));

    // LoRa init
    lora_init();


    // Tasks
  
    xTaskCreate(now_rx_task,   "now_rx",   4096, NULL, 6, NULL);
    xTaskCreate(now_tx_task,   "now_tx",   4096, NULL, 4, NULL);
    xTaskCreate(lora_rx_task,  "lora_rx",  4096, NULL, 5, NULL);
    xTaskCreate(lora_tx_task,  "lora_tx",  4096, NULL, 4, NULL);
   xTaskCreate(time_sync_task,"time_sync",3072, NULL, 3, NULL);

    uint8_t mac[6]; 
    esp_wifi_get_mac(ESP_IF_WIFI_STA, mac);
    ESP_LOGI(TAG,"GW-A up | subnet=%u | MAC=%02X:%02X:%02X:%02X:%02X:%02X",
             LOCAL_SUBNET, mac[0],mac[1],mac[2],mac[3],mac[4],mac[5]);
}
