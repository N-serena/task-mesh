from plugins.registry import tool_registry
from plugins.financial.models import FetchMetricsArgs, ComputeDebtRatioArgs
import asyncio

@tool_registry.register(name="fetch_sec_metrics", schema=FetchMetricsArgs)
async def fetch_sec_metrics(args: FetchMetricsArgs) -> dict:
    """Mock tool simulating a database or API call."""
    # Simulate network latency
    await asyncio.sleep(0.5)
    
    # In a real app, you would query an API. Here, we mock the response.
    return {
        "ticker": args.ticker,
        "fiscal_year": args.fiscal_year,
        "total_debt": 50000000.0,
        "total_equity": 120000000.0
    }

@tool_registry.register(name="compute_debt_ratio", schema=ComputeDebtRatioArgs)
async def compute_debt_ratio(args: ComputeDebtRatioArgs) -> dict:
    """Calculates the debt-to-equity ratio based on extracted metrics."""
    await asyncio.sleep(0.1)
    
    if args.total_equity == 0:
        ratio = 0.0
    else:
        ratio = args.total_debt / args.total_equity
        
    return {
        "ticker": args.ticker,
        "debt_to_equity_ratio": round(ratio, 4)
    }
