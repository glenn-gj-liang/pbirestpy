import asyncio
from pbirestpy import PowerBIClient
from pbirestpy.extension.dmv import DMV
from pbirestpy.utils import RuntimeHelper


async def test_dmv_views():
    # 替换为你的真实 token 或使用 from_service_principal
    client = PowerBIClient.from_token_literal(
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Il9qTndqZVNudlRUSzhYRWRyNVFVUGtCUkxMbyIsImtpZCI6Il9qTndqZVNudlRUSzhYRWRyNVFVUGtCUkxMbyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvODFkMTZiMzUtOTIzNS00YjZlLWIzMDMtMzI4Y2Q3NTY2ODRmLyIsImlhdCI6MTc1MzAwNTQ0NywibmJmIjoxNzUzMDA1NDQ3LCJleHAiOjE3NTMwMTExMzIsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVVFBdS84WkFBQUFLS2ZRazZ3VEt2S1BHdGx1ZVV2K09XNElvYVZhK09RL1Irc1ArRDFiVjdBUHpaVFBma2dXZng5YytEVk1HYkl5a3NmODFTTXhWODRYTEI5QTNjQlFUUT09IiwiYW1yIjpbInB3ZCIsInJzYSJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImRldmljZWlkIjoiNWRmOTI3NGEtOWI2ZS00ZjA1LTgxNjgtODJhOGZiOWQ2MTQzIiwiZmFtaWx5X25hbWUiOiLmooEiLCJnaXZlbl9uYW1lIjoi5bm_5bO7IiwiaWR0eXAiOiJ1c2VyIiwiaXBhZGRyIjoiMjQwZTozOTg6NGMwOjNiYjA6ODJiOjU0OWE6NzJjMTpjOWJmIiwibmFtZSI6IuaigeW5v-WzuyIsIm9pZCI6ImU5Y2FhOTEzLTZlM2YtNDdlNS1hZThhLWE2MzgzZmY5MWQ0NiIsInB1aWQiOiIxMDAzMjAwNDMxRjNFRjEwIiwicmgiOiIxLkFiNEFOV3ZSZ1RXU2JrdXpBektNMTFab1R3a0FBQUFBQUFBQXdBQUFBQUFBQUFDLUFPeS1BQS4iLCJzY3AiOiJBcHAuUmVhZC5BbGwgQ2FwYWNpdHkuUmVhZC5BbGwgQ2FwYWNpdHkuUmVhZFdyaXRlLkFsbCBDb25uZWN0aW9uLlJlYWQuQWxsIENvbm5lY3Rpb24uUmVhZFdyaXRlLkFsbCBDb250ZW50LkNyZWF0ZSBEYXNoYm9hcmQuUmVhZC5BbGwgRGFzaGJvYXJkLlJlYWRXcml0ZS5BbGwgRGF0YWZsb3cuUmVhZC5BbGwgRGF0YWZsb3cuUmVhZFdyaXRlLkFsbCBEYXRhc2V0LlJlYWQuQWxsIERhdGFzZXQuUmVhZFdyaXRlLkFsbCBHYXRld2F5LlJlYWQuQWxsIEdhdGV3YXkuUmVhZFdyaXRlLkFsbCBJdGVtLkV4ZWN1dGUuQWxsIEl0ZW0uRXh0ZXJuYWxEYXRhU2hhcmUuQWxsIEl0ZW0uUmVhZFdyaXRlLkFsbCBJdGVtLlJlc2hhcmUuQWxsIE9uZUxha2UuUmVhZC5BbGwgT25lTGFrZS5SZWFkV3JpdGUuQWxsIFBpcGVsaW5lLkRlcGxveSBQaXBlbGluZS5SZWFkLkFsbCBQaXBlbGluZS5SZWFkV3JpdGUuQWxsIFJlcG9ydC5SZWFkV3JpdGUuQWxsIFJlcHJ0LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWRXcml0ZS5BbGwgVGFnLlJlYWQuQWxsIFRlbmFudC5SZWFkLkFsbCBUZW5hbnQuUmVhZFdyaXRlLkFsbCBVc2VyU3RhdGUuUmVhZFdyaXRlLkFsbCBXb3Jrc3BhY2UuR2l0Q29tbWl0LkFsbCBXb3Jrc3BhY2UuR2l0VXBkYXRlLkFsbCBXb3Jrc3BhY2UuUmVhZC5BbGwgV29ya3NwYWNlLlJlYWRXcml0ZS5BbGwiLCJzaWQiOiIwMDJlNDNkOS04OTIwLWU2ZjItNTY4ZC0wNTQwMmZlN2U3ZTEiLCJzaWduaW5fc3RhdGUiOlsia21zaSJdLCJzdWIiOiI2VGFNMHgzcFNkbmEtbDVmajZPY2pESDM4dlBkMlJVaDJxRW1nSTFlbDRnIiwidGlkIjoiODFkMTZiMzUtOTIzNS00YjZlLWIzMDMtMzI4Y2Q3NTY2ODRmIiwidW5pcXVlX25hbWUiOiJndWFuZ2p1bi5saWFuZ0BkYXRhYmkuY2MiLCJ1cG4iOiJndWFuZ2p1bi5saWFuZ0BkYXRhYmkuY2MiLCJ1dGkiOiJnN1kxd2JpVkZVcVRQVzZJd1VobkFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyJiMGY1NDY2MS0yZDc0LTRjNTAtYWZhMy0xZWM4MDNmMTJlZmUiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2Z0ZCI6Ijc0b3E1YlBWUkYyaGMySWo4Nm1aaXpCWTU5TmJoU3lEbUJ1aTBJTXowWWtCYTI5eVpXRmpaVzUwY21Gc0xXUnpiWE0iLCJ4bXNfaWRyZWwiOiIyNCAxIn0.MohWW7epE2_V5NcNAgUaG0O5huSjzqfOtEdoSkF_jA_lqMJJbB5hKQh8OU5Ufq9Wiy8Emn68JfgCIVT2P2E1jPjE3HJhQdF3oL0XNb6_eZm5ayqBz2b2AJT-v4fyIRKwgu1MQIBs93a6_5l7v6qA936ulV_RF5tI-DkFzOwhh_vlGYfOf5F292YYWO3rtnHQaA4-raqjb17VpzG0x1S0l1k5Id9Bnh8bPWqOPNSr_ushdQ7ycQvrwnrWG2qlz7owCVzsUfhKjqfTdvomAoqQ6mPIi3VzIwtOnlj9BWrctlkUNhDAqofpPqaltyc2T-Z0DDlef4kOrc0LmzBjACK_Mg"
    )
    dmv = DMV(client)

    views = {
        "groups": dmv.groups,
        "datasets": dmv.datasets,
        "dataflows": dmv.dataflows,
        "reports": dmv.reports,
        "pages": dmv.pages,
        "schedules": dmv.schedules,
        "dataset_refresh_history": dmv.dataset_refresh_history,
        "dataflow_refresh_history": dmv.dataflow_refresh_history,
    }

    for name, func in views.items():
        print(f"\n=== {name.upper()} ===")
        try:
            df = await func()
            if RuntimeHelper.is_on_databricks():
                df.show(10, truncate=False)  # Spark
            else:
                print(df.head(10))  # pandas
        except Exception as e:
            print(f"[{name}] failed with error: {e}")


if __name__ == "__main__":
    asyncio.run(test_dmv_views())
