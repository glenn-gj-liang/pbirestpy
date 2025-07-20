# pbirestpy

> An asynchronous Python SDK for interacting with the Power BI REST API.

`pbirestpy` provides a fully async and object-oriented interface for interacting with Power BI workspaces, datasets, dataflows, reports, refreshes, and schedules. It supports typed resources, robust retry logic, and convenient conversion to pandas and Spark DataFrames.

---

## 🚀 Features

- ✅ Fully async and `aiohttp`-based
- ✅ Service Principal and static token authentication
- ✅ Automatic retry for 429 (Rate Limit) and 409 (Conflict) errors
- ✅ Object models for Groups, Datasets, Reports, Dataflows, etc.
- ✅ Supports refresh triggering, monitoring, and cancellation
- ✅ Convert API results to pandas or Spark DataFrames

---

## 📦 Installation

```bash
pip install pbirestpy
```

---

## 🔐 Authentication

### Service Principal

```python
from pbirestpy import PowerBIClient

client = PowerBIClient.from_service_principal(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret"
)
```

### Static Token

```python
from pbirestpy import PowerBIClient

client = PowerBIClient.from_token_literal(access_token="your-access-token")
```

---

## ⚡ Quick Start

```python
import asyncio
from pbirestpy import PowerBIClient

async def main():
    client = PowerBIClient.from_token_literal("your-token")

    async with client as session:
        groups = await session.list_groups()
        datasets = await session.list_datasets(*groups)
        for ds in datasets:
            print(ds.name)

asyncio.run(main())
```

---

## 🔁 Refreshing Datasets or Dataflows

```python
async with client as session:
    dataset = await session.get_dataset("SalesGroup", "SalesDataset")
    await session.refresh(dataset, force=True, wait_until_complete=True)
```

Includes automatic retry with:

- `429 Too Many Requests` — uses `Retry-After` header
- `409 Conflict` — supports auto-cancellation of existing refresh

---

## 📊 Convert to DataFrame

```python
from pbirestpy import PowerBIClient

# For pandas
df = PowerBIClient.to_df(await session.list_reports(...))

# For Spark (Databricks Runtime only)
spark_df = PowerBIClient.to_spark_df(await session.list_datasets(...))
```

---

## 🧩 Supported Operations

| Resource   | Supported Operations                                       |
|------------|------------------------------------------------------------|
| Group      | List, Get by name                                          |
| Dataset    | List, Get, Refresh, Get Schedules                          |
| Dataflow   | List, Get, Refresh                                         |
| Report     | List, Get, Get Pages                                       |
| Schedule   | View                                                       |
| Refresh    | Trigger, Cancel, Monitor, View history                     |

---

## 📁 Project Structure

```
pbirestpy/
├── auth/           # Authentication strategies
├── client/         # PowerBIClient & ApiSession
├── resources/      # Group, Dataset, Report, etc.
├── utils/          # Logger, datetime helpers
├── tests/          # Optional test cases
```

---

## 🧪 Development

```bash
git clone https://github.com/glenn-gj-liang/pbirestpy.git
cd pbirestpy
```

---

## 📄 License

This project is licensed under the MIT License. See [`LICENSE`](./LICENSE) for details.

---

## 🤝 Contributing

Contributions, issues and feature requests are welcome.  
Please open an issue or submit a pull request.

---

## 📬 Contact

For questions or support, contact: [guangjun_l@icloud.com](mailto:guangjun_l@icloud.com)