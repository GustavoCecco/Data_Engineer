import requests
import pandas as pd
from IPython.display import display

api_key = ""
url = f"https://api.themoviedb.org/3/movie/top_rated?api_key=65bbe4541741cb7689737423fc7be923&amp;language=pt-BR"

response = requests.get(url)
data = response.json()
filmes = []

for movie in data['results']:
    df = {'Titulo': movie['title'],
          'Data de Lançamento': movie['release_date'],
          'Visão Geral': movie['overview'],
          'Votos': movie['vote_count'],
          'Média de votos': movie['vote_average']}
    filmes.append(df)
df = pd.DataFrame(filmes)
display(df)


