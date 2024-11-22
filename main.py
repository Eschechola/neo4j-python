from neo4j import GraphDatabase
import csv
import re
import ast

uri = "neo4j+s://5379d090.databases.neo4j.io"
username = "neo4j"
password = "ntSSsgw8XfO1a2D5rkSd8k0Hn8WbtMtS5x2i0IxuT5Y"

driver = GraphDatabase.driver(uri, auth=(username, password))

def process_list(field):
    return [item.strip() for item in field.split(',') if item.strip()]

def process_list_of_dicts(field):
    try:
        return ast.literal_eval(field)
    except (SyntaxError, ValueError):
        return []

def insert_pokemon(tx, pokemon):
    peso_str = pokemon['pokemon_peso']
    peso_match = re.search(r"(\d+(\.\d+)?)", peso_str)
    peso = float(peso_match.group(0)) if peso_match else None

    tx.run("""
        MERGE (p:Pokemon {id: $pokemon_id})
        SET p.name = $pokemon_name,
            p.altura = $pokemon_altura,
            p.peso = $pokemon_peso,
            p.url_pagina = $url_pagina
        WITH p
        UNWIND $pokemon_tipos AS tipo
        MERGE (t:Tipo {name: tipo})
        MERGE (p)-[:TEM_TIPO]->(t)
        WITH p
        UNWIND $pokemon_habilidades AS habilidade
        MERGE (h:Habilidade {name: habilidade.nome, url: habilidade.url})
        MERGE (p)-[:TEM_HABILIDADE]->(h)
        WITH p
        UNWIND $pokemon_proximas_evolucoes AS evolucao
        MERGE (e:Pokemon {id: evolucao.numero, name: evolucao.nome, url: evolucao.url})
        MERGE (p)-[:EVOLUI_PARA]->(e)
        """, 
        pokemon_id=pokemon['pokemon_id'],
        pokemon_name=pokemon['pokemon_name'],
        pokemon_altura=pokemon['pokemon_altura'],
        pokemon_peso=peso,
        url_pagina=pokemon['url_pagina'],
        pokemon_tipos=pokemon['pokemon_tipos'],
        pokemon_habilidades=pokemon['pokemon_habilidades'],
        pokemon_proximas_evolucoes=pokemon['pokemon_proximas_evolucoes']
    )

def insert_data_to_neo4j(pokemons):
    with driver.session() as session:
        for pokemon in pokemons:
            session.execute_write(insert_pokemon, pokemon)

with open('data.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    pokemons = []

    for row in reader:
        row['pokemon_tipos'] = process_list(row['pokemon_tipos'])
        row['pokemon_habilidades'] = process_list_of_dicts(row['pokemon_habilidades'])
        row['pokemon_proximas_evolucoes'] = process_list_of_dicts(row['pokemon_proximas_evolucoes'])

        pokemons.append(row)

    insert_data_to_neo4j(pokemons)

driver.close()