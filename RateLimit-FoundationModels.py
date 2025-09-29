from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    AiGatewayConfig, 
    AiGatewayRateLimit, 
    AiGatewayRateLimitKey,
    AiGatewayRateLimitRenewalPeriod
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Databricks workspace client
w = WorkspaceClient()

# Define approved foundation models (using endpoint names)
APPROVED_MODELS = [
    "databricks-meta-llama-3-1-70b-instruct",
    "databricks-meta-llama-3-1-405b-instruct",
    "databricks-claude-sonnet-4",
    # Add other approved foundation model endpoint names here
]

def is_foundation_model_endpoint(endpoint):
    """
    Check if an endpoint is a foundation model endpoint.
    Foundation model endpoints have names starting with 'databricks-'.
    """
    try:
        # Check the endpoint name itself
        if endpoint.name and endpoint.name.startswith('databricks-'):
            return True, endpoint.name
        return False, None
    except Exception as e:
        logger.warning(f"Error checking endpoint {endpoint.name}: {e}")
        return False, None

def set_ai_gateway_rate_limit_to_zero(endpoint_name):
    """
    Enable AI Gateway and set rate limits to zero for a foundation model endpoint.
    """
    try:
        logger.info(f"Setting AI Gateway rate limits to zero for endpoint: {endpoint_name}")
        
        # Configure AI Gateway with rate limits set to zero at the endpoint level
        # Set both QPM (queries) and TPM (tokens) to zero
        ai_gateway_config = AiGatewayConfig(
            guardrails=None,  # No guardrails
            inference_table_config=None,  # No inference table
            rate_limits=[
                # QPM rate limit
                AiGatewayRateLimit(
                    key=AiGatewayRateLimitKey.ENDPOINT,
                    renewal_period=AiGatewayRateLimitRenewalPeriod.MINUTE,
                    calls=0  # QPM = 0 (queries per minute)
                )
            ],
            usage_tracking_config=None
        )
        
        # Update the endpoint to enable AI Gateway with zero rate limits
        w.serving_endpoints.put_ai_gateway(
            name=endpoint_name,
            guardrails=ai_gateway_config.guardrails,
            inference_table_config=ai_gateway_config.inference_table_config,
            rate_limits=ai_gateway_config.rate_limits,
            usage_tracking_config=ai_gateway_config.usage_tracking_config
        )
        
        logger.info(f"✓ Successfully set AI Gateway rate limits to zero for {endpoint_name}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error setting AI Gateway for {endpoint_name}: {e}")
        return False

def ensure_ai_gateway_enabled(endpoint_name):
    """
    Ensure AI Gateway is enabled for approved endpoints (optional - for monitoring/logging).
    You can configure normal rate limits here if desired.
    """
    try:
        logger.info(f"Ensuring AI Gateway is properly configured for approved endpoint: {endpoint_name}")
        
        # Get current endpoint to check if AI Gateway is already configured
        endpoint = w.serving_endpoints.get(endpoint_name)
        
        if endpoint.ai_gateway:
            logger.info(f"  AI Gateway already configured for {endpoint_name}")
            return True
        else:
            logger.info(f"  AI Gateway not configured - you may want to enable it for monitoring")
            # Optionally enable AI Gateway with reasonable limits for approved models
            # For now, we'll just log and skip
            return True
        
    except Exception as e:
        logger.error(f"Error checking AI Gateway for {endpoint_name}: {e}")
        return False

def main():
    """
    Main function to iterate through all endpoints and restrict non-approved foundation models.
    """
    logger.info("Starting foundation model endpoint restriction process using AI Gateway...")
    logger.info(f"Approved models: {APPROVED_MODELS}")
    logger.info("=" * 60)
    
    restricted_count = 0
    approved_count = 0
    error_count = 0
    
    try:
        # List all serving endpoints
        endpoints = w.serving_endpoints.list()
        
        for endpoint in endpoints:
            logger.info(f"\nProcessing endpoint: {endpoint.name}")
            
            # Check if it's a foundation model endpoint
            is_fm, model_name = is_foundation_model_endpoint(endpoint)
            
            if is_fm:
                logger.info(f"  → Found foundation model endpoint: {endpoint.name}")
                
                # Check if model is in approved list
                if model_name in APPROVED_MODELS:
                    logger.info(f"  ✓ Model is APPROVED - skipping restriction")
                    if ensure_ai_gateway_enabled(endpoint.name):
                        approved_count += 1
                    else:
                        error_count += 1
                else:
                    logger.warning(f"  ✗ Model is NOT APPROVED - setting rate limits to zero")
                    if set_ai_gateway_rate_limit_to_zero(endpoint.name):
                        restricted_count += 1
                    else:
                        error_count += 1
            else:
                logger.info(f"  → Skipping (not a foundation model endpoint)")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY:")
        logger.info(f"✓ Approved endpoints (unchanged): {approved_count}")
        logger.info(f"✗ Restricted endpoints (rate limit = 0): {restricted_count}")
        logger.info(f"⚠ Errors: {error_count}")
        logger.info("=" * 60)
        
        if restricted_count > 0:
            logger.info(f"\n⚠️  {restricted_count} foundation model endpoint(s) now have rate limits set to zero.")
            logger.info("   Users attempting to access these endpoints will receive rate limit errors.")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()