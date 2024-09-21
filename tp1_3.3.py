import psycopg2
import configparser
import matplotlib.pyplot as plt
import pandas as pd

# Função para ler a configuração do arquivo 'config.ini'
def get_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

# Função para conectar ao banco de dados
def connect_to_database(dbname):
    config = get_config()
    try:
        conexao = psycopg2.connect(
            host=config['postgresql']['host'],
            user=config['postgresql']['user'],
            password=config['postgresql']['password'],
            dbname=dbname,
            port=config['postgresql']['port']
        )
        return conexao
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Funções de consulta (atualizadas conforme melhorias)
def listar_comentarios_uteis(conexao, asin):
    cursor = conexao.cursor()
    try:
        # Comentários mais úteis com maior avaliação
        cursor.execute("""
            SELECT customer, rating, votes, helpful
            FROM review
            WHERE asin = %s
            ORDER BY rating DESC, helpful DESC
            LIMIT 5;
        """, (asin,))
        maior_avaliacao = cursor.fetchall()

        # Comentários mais úteis com menor avaliação
        cursor.execute("""
            SELECT customer, rating, votes, helpful
            FROM review
            WHERE asin = %s
            ORDER BY rating ASC, helpful DESC
            LIMIT 5;
        """, (asin,))
        menor_avaliacao = cursor.fetchall()

        return maior_avaliacao, menor_avaliacao, ["Customer", "Rating", "Votes", "Helpful"]
    except Exception as e:
        print(f"Erro ao listar comentários úteis: {e}")
        return [], [], []

def listar_produtos_similares(conexao, asin):
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            SELECT p2.asin, p2.salesrank
            FROM produto p1
            JOIN similare s ON p1.asin = s.asin_product
            JOIN produto p2 ON s.asin_similar = p2.asin
            WHERE p1.asin = %s AND p2.salesrank < p1.salesrank
            ORDER BY p2.salesrank ASC;
        """, (asin,))
        return cursor.fetchall(), ["ASIN Similar", "Salesrank"]
    except Exception as e:
        print(f"Erro ao listar produtos similares: {e}")
        return [], []

def mostrar_evolucao_avaliacoes(conexao, asin):
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            SELECT dt, AVG(rating) as avg_rating
            FROM review
            WHERE asin = %s
            GROUP BY dt
            ORDER BY dt;
        """, (asin,))
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=["Date", "Average Rating"])
        return df
    except Exception as e:
        print(f"Erro ao mostrar evolução das avaliações: {e}")
        return pd.DataFrame()

def listar_produtos_lideres(conexao):
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            SELECT asin, grp, salesrank
            FROM (
                SELECT asin, grp, salesrank,
                       ROW_NUMBER() OVER (PARTITION BY grp ORDER BY salesrank ASC) as rn
                FROM produto
                WHERE salesrank > 0
            ) sub
            WHERE rn <= 10 
            ORDER BY grp, salesrank;
        """)
        return cursor.fetchall(), ["ASIN", "Group", "Salesrank"]
    except Exception as e:
        print(f"Erro ao listar produtos líderes: {e}")
        return [], []

def listar_produtos_melhores_avaliacoes(conexao):
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            SELECT r.asin, AVG(r.helpful) AS avg_helpful
            FROM review r
            WHERE r.helpful > 0
            GROUP BY r.asin
            ORDER BY avg_helpful DESC
            LIMIT 10;
        """)
        return cursor.fetchall(), ["ASIN", "Avg Helpful Votes"]
    except Exception as e:
        print(f"Erro ao listar produtos com melhores avaliações: {e}")
        return [], []

def listar_categorias_melhores_avaliacoes(conexao):
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            SELECT c.description, AVG(r.helpful) AS avg_helpful
            FROM categoria c
            JOIN produto_categoria pc ON c.id_cat = pc.id_cat
            JOIN review r ON pc.asin = r.asin
            WHERE r.helpful > 0
            GROUP BY c.description
            ORDER BY avg_helpful DESC
            LIMIT 5;
        """)
        return cursor.fetchall(), ["Category", "Avg Helpful Votes"]
    except Exception as e:
        print(f"Erro ao listar categorias com melhores avaliações: {e}")
        return [], []

def listar_clientes_mais_comentarios(conexao):
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            SELECT grp, customer, num_comentarios
            FROM (
                SELECT p.grp, r.customer, COUNT(*) AS num_comentarios,
                       ROW_NUMBER() OVER (PARTITION BY p.grp ORDER BY COUNT(*) DESC) as rn
                FROM review r
                JOIN produto p ON r.asin = p.asin
                GROUP BY p.grp, r.customer
            ) sub
            WHERE rn <= 10
            ORDER BY grp, num_comentarios DESC;
        """)
        return cursor.fetchall(), ["Group", "Customer", "Num Comments"]
    except Exception as e:
        print(f"Erro ao listar clientes com mais comentários: {e}")
        return [], []

# Funções de visualização com gráficos usando apenas Matplotlib
def exibir_grafico_evolucao_avaliacoes(df, asin):
    if not df.empty:
        plt.figure(figsize=(12, 6))
        plt.plot(df["Date"], df["Average Rating"], marker='o', linestyle='-', color='b')
        plt.title(f"Evolução das Avaliações - Produto {asin}")
        plt.xlabel("Data")
        plt.ylabel("Média das Avaliações")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print("Sem dados de evolução para exibir.")

# Função para exibir gráfico de barras horizontais ajustado dinamicamente
def exibir_grafico_top_produtos(df):
    if not df.empty:
        plt.figure(figsize=(14, 8))  # Tamanho ajustado do gráfico
        grupos = df["Group"].unique()  # Identifica os grupos únicos dinamicamente

        for grupo in grupos:
            subset = df[df["Group"] == grupo]
            # Ordenando cada subset por salesrank de forma crescente
            subset = subset.sort_values(by="Salesrank", ascending=True)
            plt.barh(subset["ASIN"], subset["Salesrank"], label=grupo, height=0.6)

        plt.title("Top 10 Produtos Líderes de Venda por Grupo", fontsize=14)
        plt.ylabel("ASIN", fontsize=12)
        plt.xlabel("Posição de Vendas (Ranking)", fontsize=12)

        # Invertendo o eixo Y para que o menor ranking apareça no topo
        plt.gca().invert_yaxis()

        # Aumentar o tamanho dos rótulos do eixo Y
        plt.yticks(fontsize=10)

        # Adicionar grade ao gráfico (somente no eixo X)
        plt.grid(True, axis='x', linestyle='--', alpha=0.6)

        # Posicionar a legenda fora do gráfico para não sobrepor
        plt.legend(title="Grupo", bbox_to_anchor=(1.05, 1), loc='upper left')

        plt.tight_layout()
        plt.show()
    else:
        print("Sem dados para exibir.")





# Função para exibir resultados com pandas DataFrame
def exibir_resultado(resultados, colunas):
    if resultados:
        df = pd.DataFrame(resultados, columns=colunas)
        print(df.to_string(index=False))
        return df
    else:
        print("Nenhum resultado encontrado.")
        return pd.DataFrame()

# Função principal para exibir o menu de interação
def menu_interativo():
    conexao = connect_to_database('produtosAmazon_BD_alexandre_marcello')
    if not conexao:
        print("Erro ao conectar ao banco de dados.")
        return

    while True:
        print("\n--- Menu Dashboard ---")
        print("1. Listar os 5 comentários mais úteis e com maior e menor avaliação")
        print("2. Listar produtos similares com maiores vendas")
        print("3. Mostrar evolução diária das avaliações (Gráfico disponível)")
        print("4. Listar 10 produtos líderes de venda (Gráfico disponível)")
        print("5. Listar os 10 produtos com a maior média de avaliações úteis")
        print("6. Listar as 5 categorias com a maior média de avaliações úteis")
        print("7. Listar os 10 clientes que mais fizeram comentários por grupo de produto")
        print("8. Sair")

        escolha = input("\nEscolha uma opção: ")

        if escolha == '1':
            asin = input("Digite o ASIN do produto: ")
            maior, menor, colunas = listar_comentarios_uteis(conexao, asin)
            print("\nComentários com maior avaliação:")
            exibir_resultado(maior, colunas)
            print("\nComentários com menor avaliação:")
            exibir_resultado(menor, colunas)

        elif escolha == '2':
            asin = input("Digite o ASIN do produto: ")
            similares, colunas = listar_produtos_similares(conexao, asin)
            exibir_resultado(similares, colunas)

        elif escolha == '3':
            asin = input("Digite o ASIN do produto: ")
            df_evolucao = mostrar_evolucao_avaliacoes(conexao, asin)
            if not df_evolucao.empty:
                print(df_evolucao.to_string(index=False))
                mostrar_grafico = input("\nDeseja exibir o gráfico (s/n)? ")
                if mostrar_grafico.lower() == 's':
                    exibir_grafico_evolucao_avaliacoes(df_evolucao, asin)
            else:
                print("Sem dados para exibir.")

        elif escolha == '4':
            resultados, colunas = listar_produtos_lideres(conexao)
            df_top_produtos = exibir_resultado(resultados, colunas)
            mostrar_grafico = input("\nDeseja exibir o gráfico (s/n)? ")
            if mostrar_grafico.lower() == 's':
                exibir_grafico_top_produtos(df_top_produtos)

        elif escolha == '5':
            melhores_produtos, colunas = listar_produtos_melhores_avaliacoes(conexao)
            exibir_resultado(melhores_produtos, colunas)

        elif escolha == '6':
            melhores_categorias, colunas = listar_categorias_melhores_avaliacoes(conexao)
            exibir_resultado(melhores_categorias, colunas)

        elif escolha == '7':
            mais_comentarios, colunas = listar_clientes_mais_comentarios(conexao)
            exibir_resultado(mais_comentarios, colunas)

        elif escolha == '8':
            print("Saindo...")
            conexao.close()
            break

        else:
            print("Opção inválida. Tente novamente.")

# Executar o menu interativo
if __name__ == '__main__':
    menu_interativo()
