from finhack.library.config import Config
from runtime.constant import *
from openai import OpenAI
import os

class AI:
    def load_prompt(prompt_name):
        path=f"{BASE_DIR}/prompt/{prompt_name}"
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as file:
                return file.read()
        return ""
        
    def ChatGPT(prompt,model=""):
        cfgAI=Config.get_config('ai','chatgpt')
        
        if model=="":
            model=cfgAI['model']
        
        client = OpenAI(
            base_url=cfgAI['base_url'],
            api_key=cfgAI['api_key'],
            max_retries=int(cfgAI['max_retries'])
        )
        
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model=model
        )
        return chat_completion.choices[0].message.content 
