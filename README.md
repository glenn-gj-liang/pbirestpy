# pbirestpy

`pbirestpy` is a lightweight and async-friendly Python client for the [Microsoft Power BI REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/). It is designed to simplify integration into data pipelines, automation tools, and analytics platforms by wrapping common Power BI operations such as authentication, dataset management, and refresh monitoring.

---

## ✨ Features

- 🔐 Supports Azure AD authentication (client credentials flow)
- ⚡ Asynchronous I/O using `aiohttp`
- 📊 Fetch datasets, refresh history, and groups (workspaces)
- 📥 Trigger dataset refresh operations
- 🧾 Convert API responses to Pandas DataFrame
- ⚙️ Easy to integrate into ETL, scripting, or notebook environments

---

## 📦 Installation

Install via pip (once published):

```bash
pip install pbirestpy
