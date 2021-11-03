import sqlite3 as conector
import pandas

conexao = None
cursor = None

try:
    conexao = conector.connect("./meu_banco.db")
    conexao.execute("PRAGMA foreign_keys = on")
    cursor = conexao.cursor()

    comando = '''SELECT Municipio.nome, Dengue.casos, Populacao.populacao
                    FROM Municipio
                    JOIN Dengue ON Municipio.codigo = Dengue.codigo
                    JOIN populacao ON Municipio = populacao.codigo
                    WHERE  Dengue.ano = :ano AND Populacao.ano=:ano'''
    ano = {"ano":2018}
    cursor.execute(comando, ano)

    #Recuperação e Dados
    registros = cursor.fetchall()

    resultado = pandas.read_sql(sql=comando, con=conexao, params=ano)
    resultado ['incidenci'] = 100 * resultado['casos'] / resultado['populacao']
    print(resultado)
    print(resultado['incidencia'].describe())
    print(resultado.loc[resultado['incidencia'].idxmax()])

    conexao.commit()

except conector.OperationalError as erro:
    print("Erro Operacional", erro)
except conector.IntegrityError as erro:
    print("Erro de banco de Dados", erro)
finally:
    cursor.close()
    conexao.close()



