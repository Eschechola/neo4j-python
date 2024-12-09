# Documentação usada para fazer o EP2 de Ciencia de Dados:
# https://neo4j.com/docs/python-manual/current/
# https://neo4j.com/docs/python-manual/current/query-simple/
# https://neo4j.com/docs/python-manual/current/transactions/

import json
from neo4j import GraphDatabase

URI = "neo4j+s://7f9eb2ad.databases.neo4j.io"
AUTH = ("neo4j", "SUsG333S9C5m7jknNP-Jp0FNazn724L02x6yR4aG8F0")

def criar_no_pokemon():
    return """
        MERGE (p:Pokemon {id: $pid})
            SET
                p.nome = $nome,
                p.altura = $altura,
                p.peso = $peso,
                p.url = $url
        """


def criar_no_tipos():
    return """
        WITH p
        UNWIND $tipos AS tipo
        MERGE (t:Tipo {name: tipo})
        MERGE (p)-[:E_DO_TIPO]->(t)
    
    """


def criar_no_habilidades():
    return """
        WITH p
        UNWIND $habilidades AS habilidade
        MERGE (h:Habilidade {name: habilidade.nome, url: habilidade.url})
        MERGE (p)-[:POSSUI_HABILIDADE]->(h)
    
    """


def criar_no_evolucao():
    return """
        WITH p
        UNWIND $evolucoes AS evolucao
        MERGE (e:Pokemon {id: evolucao.numero, name: evolucao.nome, url: evolucao.url})
        MERGE (p)-[:TEM_EVOLUCAO]->(e)
    
    """


def inserir_pokemon(tx, pokemon):
    query = str(criar_no_pokemon() + 
            criar_no_tipos() +
            criar_no_habilidades() +
            criar_no_evolucao())

    altura_formatada = pokemon['pokemon_altura'].split('Â')[0]
    peso_formatado = pokemon['pokemon_peso'].split('Â')[0]
    habilidades_formatada = pokemon['pokemon_habilidades'].replace("'", '"')
    evolucoes_formatada = pokemon['pokemon_proximas_evolucoes'].replace("'", '"')

    tx.run(query, 
        pid = pokemon['pokemon_id'], 
        nome = pokemon['pokemon_name'],
        altura = float(altura_formatada),
        peso = float(peso_formatado),
        url = pokemon['url_pagina'],
        tipos = pokemon['pokemon_tipos'],
        habilidades = json.loads(habilidades_formatada),
        evolucoes = json.loads(evolucoes_formatada))
    

def inserir_pokemons(session):
    with open('pokemons.json', 'r') as file:
        data = json.load(file)
        for pokemon in data['pokemons']:
            try:
                session.execute_write(inserir_pokemon, pokemon)
                print("Pokemon: " + str(pokemon['pokemon_id']) + " - " + str(pokemon['pokemon_name']) + ' inserido com sucesso!')
            except:
                print("Os dados do pokemon: " + str(pokemon['pokemon_id']) + " - " + str(pokemon['pokemon_name']) + ' estão inválidos!')

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session(database="neo4j") as session:
        inserir_pokemons(session)