import google.generativeai as genai
import os
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY", ""))

def test_tool():
    """Just a test tool"""
    return "test"

model = genai.GenerativeModel("gemini-2.5-flash", tools=[test_tool])
chat = model.start_chat(enable_automatic_function_calling=True)
res = chat.send_message("Use the tool")
print("Response text:", res.text)
print("Parts:", res.parts)
