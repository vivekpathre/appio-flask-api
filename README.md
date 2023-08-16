# Apollo.io Data Retrieval Flask App

This is a Flask web application designed to fetch and process data from the Apollo.io API. The application connects to a PostgreSQL database and retrieves data based on specific API requests.

## Features

- Retrieves data from Apollo.io API based on user requests.
- Stores fetched data in a PostgreSQL database.
- Supports fetching both email and contact details for a given person.
- Handles different scenarios based on API key and user request parameters.

## Prerequisites

Before running the application, ensure you have the following:

- Python installed (recommended version: 3.x).
- Required Python libraries: `ast`, `re`, `json`, `psycopg2`, `requests`, `flask`.

## Setup

1. Install the required Python libraries using the following command:

   ```bash
   pip install ast re json psycopg2 requests flask
