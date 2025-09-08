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

---

```bash
pip install -r requirements.txt
```
After that, simply run the docker compose file 

```bash
docker compose up -d
```
Open your browser in the port [3000](http://localhost:3000/)
```bash
Login: admin
Password: admin
```
Navigate to the dashboards pannel on the left and click on _Solar Power Plant_.
Next, simply run:
```bash
python3 /sim_core/main.py
```
Now, in 10 seconds be able to see the dashboard working. If not, check the refresh button on the top right corner in grafana.
