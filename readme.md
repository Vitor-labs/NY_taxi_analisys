# PROJETO FINAL SISTEMAS DISTRIBUIDOS

##  <center> **Definição do sistema** </center>

### <u>Conceitos Envolvidos:</u>
- Envio de dados utilizando Sockets TCP e Datagrama UDP
- Protocolo Requisição/Resposta.
- Representação externa de dados. JSON e Protocol Buffers
- Comunicação por filas: implantar Apache Kafka
  
### <u>Arquitetura:</u>

- Aplicação
  
<img alt="arquitetura da aplicação distribuida" align="center" src="assets/arctecture.drawio.svg">

- Dados

<img alt="arquitetura dos dados" align="center" src="assets/data_models.drawio.svg">

### <u>Requisitos Não-Funcionais:</u>

**[ ]** Privacidade: criptografia nas transmições de dados e requests

**[ ]** Confiabilidade: implementações da GCP / estado das filas com Apache Kafka

**[ X ]** Escalabilidade: implementações da GCP

----
## <center> **Como Executar este Projeto** </center>

1. Crie uma rede no Docker
```sh
docker network create pg-network
```
2. Construa a imagem do docker
```sh
docker build -t <container_name> .
```
3. Execute a imagem
```sh
docker run -it --network=pg-network data_ingest:v001 --u=root --pw=root --h=localhost --p=5432 --db=ny_taxi --t=yellow_taxi_data --U={url}
```

## Apresentação

- Descrição do sistema a ser implementado:
  - Sistema de Coleta, Transformação e Visualização de dados de viagens de taxi amarelo da cidade de nova yorke

- Como vocês pretendem implementar os seguintes módulos: 
  - comunicação (tcp, udp, filas): via Apache Kafka
  - representacao externa de dados (json, protocol buffer): Json
  - protocolo requisição-resposta (já mostrar a especificação do protocolo e como ele está até o momento)

- Quais requisitos não-funcionais vocês pretendem implementar

- Uma arquitetura inicial do sistema a ser construído
  - Já definida
