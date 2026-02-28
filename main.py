from fastapi import FastAPI
from fastapi import Request
import json

app = FastAPI()

@app.get("/")
def home():
    return {"status": "Lead Engine Live"}

@app.post("/vapi/webhook")
async def webhook(request: Request):
    body = await request.json()

    message = body.get("message", {})
    tool_calls = message.get("toolCallList", [])

    results = []

    for tool in tool_calls:
        tool_name = tool.get("name")
        tool_id = tool.get("id")

        if tool_name == "setLeadCriteria":
            results.append({
                "name": tool_name,
                "toolCallId": tool_id,
                "result": json.dumps({"status": "criteria saved"})
            })

        elif tool_name == "runDailyLeadRun":
            results.append({
                "name": tool_name,
                "toolCallId": tool_id,
                "result": json.dumps({
                    "status": "daily leads generated",
                    "hvac": 10,
                    "dispensary": 5,
                    "gym": 5
                })
            })

        elif tool_name == "previewTodaysLeads":
            results.append({
                "name": tool_name,
                "toolCallId": tool_id,
                "result": json.dumps({"preview": "20 leads ready"})
            })

        elif tool_name == "sendDailyLeadEmail":
            results.append({
                "name": tool_name,
                "toolCallId": tool_id,
                "result": json.dumps({"status": "email sent"})
            })

    return {"results": results}
