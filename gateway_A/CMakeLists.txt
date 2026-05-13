idf_component_register(
    SRCS "main.c"
    INCLUDE_DIRS "."
    REQUIRES
        common
        net_time
        lora_driver
        esp_wifi
        
        nvs_flash
        esp_netif
        freertos
)

