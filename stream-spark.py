import streamlit as st
import requests
import pandas as pd
import json

# Personalización con CSS
st.markdown("""
    <style>
        .big-font {
            font-size:30px !important;
            color: #2C3E50;
        }
        .name-font {
            font-size:20px;
            color: #2C3E50;
        }
        .header-font {
            font-size: 24px !important;
            color: #16A085;
        }
        .stButton>button {
            background-color: white;
            color: #16A085;
            font-size: 18px;
            border-radius: 8px;
            padding: 10px;
        }
        .stTextInput>div>div>input {
            background-color: #f0f0f5;
            border: 2px solid #BDC3C7;
        }
    </style>
""", unsafe_allow_html=True)

# Funciones para POST y GET
def post_spark_job(user, repo, job, token):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    payload = {"event_type": job}
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }

    st.write("### URL del API")
    st.write(url)
    st.write("### Payload")
    st.json(payload)
    st.write("### Headers")
    st.json(headers)

    response = requests.post(url, json=payload, headers=headers)

    st.write("### Respuesta del servidor")
    st.write(response)

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write("### Respuesta del servidor")
    st.write(response)

    if response.status_code == 200:
        st.write("### Resultados de Spark")
        st.json(response.json())
    else:
        st.write(f"Error: {response.status_code}")

# Título y descripción
st.title("Aplicación de Spark y Streamlit")
st.markdown("<p class='big-font'>Conéctate con tu repositorio de Github y envía trabajos Spark</p>", unsafe_allow_html=True)
st.markdown("<p class='name-font'>Yestli Darinka Santos Sánchez - A01736992</p>", unsafe_allow_html=True)


# Sección para enviar un job de Spark
st.header("Enviar trabajo Spark")
github_user = st.text_input('Usuario de Github', value='Yestli-Sanchez')
github_repo = st.text_input('Repositorio de Github', value='semanaTec')
spark_job = st.text_input('Trabajo Spark', value='spark')
github_token = st.text_input('Token de Github', value='***', type="password")

if st.button("Enviar trabajo Spark"):
    post_spark_job(github_user, github_repo, spark_job, github_token)

# Sección para obtener los resultados de Spark
st.header("Resultados del trabajo Spark")
url_results = st.text_input('URL de resultados', value='https://raw.githubusercontent….')

if st.button("Obtener resultados"):
    get_spark_results(url_results)
