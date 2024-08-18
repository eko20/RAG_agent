import sqlite3
import pandas as pd
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.agents.agent_toolkits import create_retriever_tool
from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.utilities import SQLDatabase
import os

# Set up API key
api_key = ""
os.environ["OPENAI_API_KEY"] = api_key

# Select the first column of the DataFrame, drop any null values, 
# and get a list of unique values
def query_as_list(conn, query):
    df = pd.read_sql_query(query, conn)
    unique_values = {}
    for column in df.columns:
        if df[column].dtype == 'object':  # Handle text columns
            unique_values[column] = df[column].dropna().unique().tolist()
        else:  # Handle numeric columns
            unique_values[column] = df[column].dropna().unique().tolist()
    return unique_values

# Connect to your SQLite database
conn = sqlite3.connect("log.db")

query = "SELECT DISTINCT geoip_country, geoip_city, device_type, user_agent, id, status FROM log"
unique_values = query_as_list(conn, query)

geoip_countries = unique_values.get('geoip_country', [])
device_types = unique_values.get('device_type', [])
user_agents = unique_values.get('user_agent', [])
ids = unique_values.get('id', [])  # This will be a list of integers
status = unique_values.get('status', [])

conn.close()



# Convert integers to strings for consistent list operations
texts = (geoip_countries  + device_types + user_agents + [str(i) for i in ids] + [str(i) for i in status])


# Create a FAISS vector store
vector_db = FAISS.from_texts(texts, OpenAIEmbeddings())
retriever = vector_db.as_retriever(search_kwargs={"k": 5})

description = """Use to look up values to filter on. Input is an approximate spelling of the proper noun, output is \
valid proper nouns. Use the noun most similar to the search."""
retriever_tool = create_retriever_tool(
    retriever,
    name="search_proper_nouns",
    description=description,
)
print(retriever)

llm = ChatOpenAI(model="gpt-4o-mini")

# Connect to your SQLite database again
conn = sqlite3.connect("log.db")
db = SQLDatabase.from_uri("sqlite:///log.db")

# Create the SQL agent with the retriever tool
agent_executor = create_sql_agent(llm, db=db, agent_type="openai-tools", tools=[retriever_tool], verbose=True)

# Define a test query to check the retriever tool
while True:
    query = input("Please enter your query (or type 'q' to quit): ")
    
    if query.lower() == 'q':
        break

    # Use the retriever tool
    try:
        retriever_results = retriever_tool.invoke({"query": query})
        print("Retriever results:", retriever_results)
    except Exception as e:
        print("Error invoking retriever tool:", e)

    # Run a query with the SQL agent
    result = agent_executor.invoke({"input": query})
    print(result)

conn.close()
