from typing import Optional

from pydantic import BaseModel, Field


class MacroClassification(BaseModel):
    """Classification result for macroeconomic indicators."""

    affects_macro_indicator: bool = Field(..., description="Whether the news affects a macro indicator")
    impact_level: str = Field(..., description="Impact level: None, Low, Moderate, or High")
    affected_category: str = Field(..., description="Affected category from the macro categories list")
    impact_frequency: str = Field(..., description="Impact frequency: One-time, Short-term, Structural, or Not Applicable")
    geographical_scope: str = Field(..., description="Geographical scope: National, Global, or None")
    reasoning: str = Field(..., description="1-2 line economic justification")


class NewsArticle(BaseModel):
    """Pydantic model for news articles from Pulse.zerodha.com."""

    heading: str = Field(..., description="The news article heading/title")
    content: str = Field(..., description="The news article content/description")
    source: Optional[str] = Field(None, description="Source of the news article")
    timestamp: Optional[str] = Field(None, description="Timestamp when the article was published")
    url: Optional[str] = Field(None, description="URL to the full article if available")
    classification: Optional[MacroClassification] = Field(None, description="Macroeconomic classification result")
