# DATAMINE-QBO_Orchestration
University assignment on building a backfill data pipeline for information extraction from QBO



## Gestión de secretos
Los secretos se encuentran cargados dentro de Mage Secrets. 

Para inicializar y proteger los valores sensibles de los contenedores QBO y PGAdmin se realizó el siguiente procedimiento:
1. Se creo un archivo docker-compose.yaml con credenciales únicas.
2. Se realizo docker compose up sobre este .yaml para inicializar los contenedores y volumenes con estas credenciales.
3. Una vez inicializados los volumenes, se remplazaron los valores del docker-compose.yaml por credenciales mockup.
4. Se subió a github esta version del docker-compose.yaml, junto a los volumenes inicializados.
