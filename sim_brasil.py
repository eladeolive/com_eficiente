import numpy as np
import pandas as pd
import polars as pl
import requests

#Random Bivariate Poi (m1,m2,0)
def bivariate_poi(m1,m2,sz):
    x = np.random.poisson(m1,sz)
    y = np.random.poisson(m2,sz)
    return list(zip(x,y))

#Simula o resultado de n jogos
def simulate_games(team1,team2,ngames=1):
    
    m1 = ((df.filter(df['Pais'].str.contains(team1))['Gf']+df.filter(df['Pais'].str.contains(team2))['Gs'])/2).to_numpy()
    m2 = ((df.filter(df['Pais'].str.contains(team2))['Gf']+df.filter(df['Pais'].str.contains(team1))['Gs'])/2).to_numpy()

    jogos = pl.DataFrame(bivariate_poi(m1,m2,ngames)).transpose().lazy()\
        .groupby(['column_0','column_1']).agg([pl.count()/ngames])\
        .with_column(pl.when(pl.col("column_0") > pl.col("column_1")).then(team1)\
                        .when(pl.col("column_0") < pl.col("column_1")).then(team2)\
                         .otherwise('empate'))

    return jogos.collect()

def simulate_fase(grupo):
    
    pontos = pd.DataFrame()
    pontos['times'] = group_g
    pontos = pontos.assign(jogos=[[],[],[],[]], gols_feitos=[0]*4 , gols_sofridos=[0]*4, pontos=[0]*4)
    pontos = pontos.set_index('times')

    for i in range(len(grupo)):
        for j in range(i+1,len(grupo)):
            jogo = simulate_games(grupo[i],grupo[j])
            if jogo['column_0'].to_list()[0] < jogo['column_1'].to_list()[0]:
                pontos.at[grupo[j],'jogos'].append('V')
                pontos.at[grupo[j],'pontos'] += 3
                pontos.at[grupo[i],'jogos'].append('D')
            elif jogo['column_0'].to_list()[0] > jogo['column_1'].to_list()[0]:
                pontos.at[grupo[i],'jogos'].append('V')
                pontos.at[grupo[i],'pontos'] += 3
                pontos.at[grupo[j],'jogos'].append('D')
            else:
                pontos.at[grupo[i],'jogos'].append('E')
                pontos.at[grupo[i],'pontos'] += 1
                pontos.at[grupo[j],'pontos'] += 1
                pontos.at[grupo[j],'jogos'].append('E')
            pontos.at[grupo[i],'gols_feitos'] += list(jogo['column_0'])[0]
            pontos.at[grupo[j],'gols_feitos'] += list(jogo['column_1'])[0]
            pontos.at[grupo[i],'gols_sofridos'] += list(jogo['column_1'])[0]
            pontos.at[grupo[j],'gols_sofridos'] += list(jogo['column_0'])[0]

    pontos['saldo_gols'] = pontos['gols_feitos'] - pontos['gols_sofridos']
    pontos = pontos.sort_values(by=['pontos','saldo_gols','gols_feitos'], ascending=False)
    pontos['classificação'] = ['classificado','classificado','eliminado','eliminado']
    return pontos

def simulacao_decisivo(nsim):
        fase = simulate_fase(group_g)
        res = pd.DataFrame(
                [1 if i == 'V' else 0 for i in fase.loc['Brazil']['jogos']] + [fase.loc['Brazil']['classificação']],
                index = ['jogo1','jogo2','jogo3','classifi']).T
        return res

def simulate_fase(grupo):
    
    pontos = pd.DataFrame()
    pontos['times'] = group_g
    pontos = pontos.assign(jogos=[[],[],[],[]], gols_feitos=[0]*4 , gols_sofridos=[0]*4, pontos=[0]*4)
    pontos = pontos.set_index('times')

    for i in range(len(grupo)):
        for j in range(i+1,len(grupo)):
            jogo = simulate_games(grupo[i],grupo[j])
            if jogo['column_0'].to_list()[0] < jogo['column_1'].to_list()[0]:
                pontos.at[grupo[j],'jogos'].append('V')
                pontos.at[grupo[j],'pontos'] += 3
                pontos.at[grupo[i],'jogos'].append('D')
            elif jogo['column_0'].to_list()[0] > jogo['column_1'].to_list()[0]:
                pontos.at[grupo[i],'jogos'].append('V')
                pontos.at[grupo[i],'pontos'] += 3
                pontos.at[grupo[j],'jogos'].append('D')
            else:
                pontos.at[grupo[i],'jogos'].append('E')
                pontos.at[grupo[i],'pontos'] += 1
                pontos.at[grupo[j],'pontos'] += 1
                pontos.at[grupo[j],'jogos'].append('E')
            pontos.at[grupo[i],'gols_feitos'] += list(jogo['column_0'])[0]
            pontos.at[grupo[j],'gols_feitos'] += list(jogo['column_1'])[0]
            pontos.at[grupo[i],'gols_sofridos'] += list(jogo['column_1'])[0]
            pontos.at[grupo[j],'gols_sofridos'] += list(jogo['column_0'])[0]

    pontos['saldo_gols'] = pontos['gols_feitos'] - pontos['gols_sofridos']
    pontos = pontos.sort_values(by=['pontos','saldo_gols','gols_feitos'], ascending=False)
    pontos['classificação'] = ['classificado','classificado','eliminado','eliminado']
    return pontos


def simulacao_decisivo(nsim):
        fase = simulate_fase(group_g)
        res = pd.DataFrame(
                [1 if i == 'V' else 0 for i in fase.loc['Brazil']['jogos']] + [fase.loc['Brazil']['classificação']],
                index = ['jogo1','jogo2','jogo3','classifi']).T
        return res

   
if __name__ == '__main__':
    from multiprocessing.pool import Pool
    #from multiprocessing import set_start_method
    #set_start_method("spawn")

    url = 'https://docs.google.com/spreadsheets/d/1IyDh4y0ul63reEMASf-kmEgxmOy1tUoM/edit?usp=share_link&ouid=117009962348251314847&rtpof=true&sd=true'
    a = requests.get(url)
    df = pd.read_html(a.content)[0]
    df.columns = df.iloc[0]

    df = pl.from_pandas(df.drop(0).drop(1,axis=1))\
    .with_column((pl.col('Gols_feitos')/pl.col('Jogos')).alias('Gf'))\
    .with_column((pl.col('Gosl_sofridos')/pl.col('Jogos')).alias('Gs'))

    group_g = ['Brazil', 'Serbia', 'Switzerland', 'Cameroon']

    brasil = pd.DataFrame().assign(jogo1='',jogo2='',jogo3='',classifi='')

    with Pool() as pool:
        try:
            for res in pool.imap_unordered(simulacao_decisivo,range(20)):
                    brasil = pd.concat([brasil, res], ignore_index=True)
        except:
            brasil.to_csv('brasil_except.csv')

    brasil.to_csv('brasil.csv')
