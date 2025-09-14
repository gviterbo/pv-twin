# Simple Digital Twin for PV Systems

---

## Prerequisites

Before starting, make sure you have the following installed:

- [Python 3.10](https://www.python.org/downloads/)
- [Docker](https://www.docker.com/products/docker-desktop)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

## Installation

First, install the project dependencies using the `requirements.txt` file:

1. Open the terminal.
2. Navigate to the project directory.
3. In your custom Python 3.10 environment, run the following command:

```bash
pip install -r requirements.txt
```
4. After that, simply run the docker compose file 

```bash
docker compose up -d
```
5. Open your browser in the port [3000](http://localhost:3000/)
```bash
Login: admin
Password: admin
```
6. Navigate to the dashboards pannel on the left and click on _Solar Power Plant_.
7. Next, simply run:
```bash
python3 /sim_core/main.py
```
8. Now, in 10 seconds you will be able to see the dashboard working. If not, check the refresh button on the top right corner in grafana.
9. To shut down the simulator, press `Ctrl` + `C` in the terminal.
10. Finally, to stop the container and get rid of the data stored during the simulation, run:
```bash
docker compose down
docker volume rm pv-twin_vm-data pv-twin_grafana-data
```
---

This project was funded by the CNPQ (Brazil's National Council for Scientific and Technological Development)
