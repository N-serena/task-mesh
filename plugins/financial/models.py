from pydantic import BaseModel, Field

class FetchMetricsArgs(BaseModel):
    """Arguments for fetching SEC financial filings."""
    ticker: str = Field(
        ..., 
        description="The official stock ticker symbol, exactly 1-5 uppercase letters (e.g., 'AAPL').",
        pattern=r"^[A-Z]{1,5}$"
    )
    fiscal_year: int = Field(
        ..., 
        description="The 4-digit fiscal year (e.g., 2024).",
        ge=2000,
        le=2100
    )

class ComputeDebtRatioArgs(BaseModel):
    """Arguments for calculating the debt-to-equity ratio."""
    ticker: str = Field(..., description="The stock ticker symbol (e.g., 'AAPL').")
    total_debt: float = Field(
        ..., 
        description="Total debt in USD. Must be a float, not a percentage or formatted string."
    )
    total_equity: float = Field(
        ..., 
        description="Total shareholder equity in USD. Must be a float."
    )
