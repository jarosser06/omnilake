

DEFAULT_RESPONSE_PROMPT = """Based on the information provided, provide a response based on the desired goal and the provided content. Follow these guidelines:
- The response should be clear.
- The response should be accurate based on the information provided.
- The response should be relevant to the request.
- Follow any specific instructions provided in the goal.
- Overcommunicate and provide as much information as possible that is relevant to the user's goal.
- DO NOT provide a statement declaring what is being presented. E.g. "Based on the information provided, the answer is..."

If you do not feel there is sufficient information to provide a response, simply specify "Insufficient information for response"
"""