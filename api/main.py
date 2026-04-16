from fastapi import FastAPI

app = FastAPI(
    title="Recon Pipeline API",
    description="Exposes financial reconciliation exceptions detected by the pipeline",
    version="0.1.0"
)

@app.get("/health")
def health_check():
    # Basic health check — confirms the API is running
    return {"status": "ok", "version": "0.1.0"}