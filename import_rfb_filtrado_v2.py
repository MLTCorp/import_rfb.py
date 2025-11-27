#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RFB ETL FILTRADO V2 - Processa arquivos COM FILTRO de CNAE
- Filtra CNAE principal OU secundário  
- INSERT direto na tabela estabelecimentos
- Batch size: 10.000 registros
- Usa execute_values() (3-5x mais rápido)
- Permite escolher quais arquivos processar
"""
import sys, urllib.request, zipfile, io, time, csv
from datetime import datetime

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import execute_values

MES_ANO = sys.argv[1] if len(sys.argv) > 1 else "2024-01"
ARQUIVO_INICIO = int(sys.argv[2]) if len(sys.argv) > 2 else 0
ARQUIVO_FIM = int(sys.argv[3]) if len(sys.argv) > 3 else 9
BATCH_SIZE = 10000

# LISTA DE CNAEs PERMITIDOS (14 CNAEs)
CNAES_PERMITIDOS = {
    '4744099', '4744005', '4679699', '4674500', '2330302', '2330399', '4744001',
    '4742300', '4743100', '4744002', '4741500', '4744004', '4744003', '4744006'
}

# Connection String (Transaction Pooler - Porta 6543)
CONN_STRING = "postgresql://postgres.oxuqbcltlykvyeaambtq:IsQ9qbndTR6VNSkI@aws-1-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require"

BASE_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{MES_ANO}"

def conectar_db(tentativas=3):
    for i in range(tentativas):
        try:
            conn = psycopg2.connect(CONN_STRING)
            conn.autocommit = False
            return conn
        except Exception as e:
            print(f"      ⚠️  Tentativa {i+1}/{tentativas} falhou: {str(e)[:60]}")
            if i < tentativas - 1:
                time.sleep(2)
            else:
                raise

def processar_arquivo(arquivo_num):
    arquivo_nome = f"Estabelecimentos{arquivo_num}.zip"
    url = f"{BASE_URL}/{arquivo_nome}"

    inicio = time.time()
    print("\n" + "="*70)
    print(f"📦 Arquivo: {arquivo_nome} (#{arquivo_num})")
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    print("\n[1/3] 📥 Baixando arquivo...")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    response = urllib.request.urlopen(req, timeout=600)
    zip_data = response.read()
    tamanho_mb = len(zip_data) / (1024 * 1024)
    print(f"      ✅ Download: {tamanho_mb:.2f}MB")

    print("\n[2/3] 📦 Descompactando...")
    zip_file = zipfile.ZipFile(io.BytesIO(zip_data))
    csv_filename = zip_file.namelist()[0]
    csv_file = io.TextIOWrapper(zip_file.open(csv_filename), encoding='latin-1')
    print(f"      ✅ CSV: {csv_filename}")
    del zip_data

    print("\n[3/3] 🔌 Conectando PostgreSQL...")
    conn = conectar_db()
    cursor = conn.cursor()
    print("      ✅ Conectado!")

    print("\n[4/4] 🚀 Filtrando e inserindo (CNAE principal OU secundário)...")

    csv_reader = csv.reader(csv_file, delimiter=';', quotechar='"')
    batch = []
    total_aceitos = 0
    total_rejeitados = 0
    batch_num = 0

    for row in csv_reader:
        row = [campo.strip('"') if campo else '' for campo in row]
        while len(row) < 30:
            row.append('')

        cnae_principal = row[11] if len(row) > 11 else ''
        cnae_secundaria = row[12] if len(row) > 12 else ''

        cnae_encontrado = cnae_principal in CNAES_PERMITIDOS

        if not cnae_encontrado and cnae_secundaria:
            cnaes_secundarios = [c.strip() for c in cnae_secundaria.split(',')]
            cnae_encontrado = any(c in CNAES_PERMITIDOS for c in cnaes_secundarios)

        if not cnae_encontrado:
            total_rejeitados += 1
            continue

        batch.append(tuple(row[:30]))
        total_aceitos += 1

        if len(batch) >= BATCH_SIZE:
            try:
                execute_values(
                    cursor,
                    """
                    INSERT INTO estabelecimentos (
                      cnpj_basico, cnpj_ordem, cnpj_dv,
                      identificador_matriz_filial, nome_fantasia,
                      situacao_cadastral, data_situacao_cadastral,
                      motivo_situacao_cadastral, nome_cidade_exterior, pais,
                      data_inicio_atividade,
                      cnae_fiscal_principal, cnae_fiscal_secundaria,
                      tipo_logradouro, logradouro, numero, complemento, bairro,
                      cep, uf, municipio,
                      ddd_1, telefone_1, ddd_2, telefone_2,
                      ddd_fax, fax, correio_eletronico,
                      situacao_especial, data_situacao_especial
                    ) VALUES %s
                    ON CONFLICT (cnpj_completo) DO NOTHING
                    """,
                    batch,
                    page_size=1000
                )
                conn.commit()
            except Exception as e:
                print(f"      ⚠️  Erro no batch {batch_num}: {str(e)[:100]}")
                conn.rollback()

            batch_num += 1
            if batch_num % 5 == 0:
                taxa = (total_aceitos / (total_aceitos + total_rejeitados) * 100) if (total_aceitos + total_rejeitados) > 0 else 0
                print(f"      📊 Aceitos: {total_aceitos:,} | Rejeitados: {total_rejeitados:,} | Taxa: {taxa:.2f}%")
            batch = []

    if batch:
        try:
            execute_values(cursor, """INSERT INTO estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial) VALUES %s ON CONFLICT (cnpj_completo) DO NOTHING""", batch, page_size=1000)
            conn.commit()
        except: conn.rollback()

    taxa_final = (total_aceitos / (total_aceitos + total_rejeitados) * 100) if (total_aceitos + total_rejeitados) > 0 else 0
    print(f"      ✅ Aceitos: {total_aceitos:,} | 🗑️ Rejeitados: {total_rejeitados:,} | 📈 Taxa: {taxa_final:.2f}%")
    csv_file.close()
    cursor.close()
    conn.close()

    tempo = int(time.time() - inicio)
    print(f"\n✅ Arquivo {arquivo_num} CONCLUÍDO! | {total_aceitos:,} aceitos | {tempo//60}min {tempo%60}s")
    return {"arquivo": arquivo_num, "aceitos": total_aceitos, "rejeitados": total_rejeitados, "tempo": tempo, "tamanho_mb": round(tamanho_mb, 2)}

def main():
    inicio_total = time.time()
    print("\n🚀 RFB ETL FILTRADO V2 - COM FILTRO DE CNAE")
    print(f"📅 {MES_ANO} | 📦 Arquivos {ARQUIVO_INICIO}-{ARQUIVO_FIM} | 🎯 {len(CNAES_PERMITIDOS)} CNAEs")

    resultados = []
    total_aceitos = 0
    total_rejeitados = 0

    for i in range(ARQUIVO_INICIO, ARQUIVO_FIM + 1):
        try:
            resultado = processar_arquivo(i)
            resultados.append(resultado)
            total_aceitos += resultado["aceitos"]
            total_rejeitados += resultado["rejeitados"]
        except Exception as e:
            print(f"\n❌ ERRO arquivo {i}: {e}")
            continue

    tempo_total = int(time.time() - inicio_total)
    taxa_geral = (total_aceitos / (total_aceitos + total_rejeitados) * 100) if (total_aceitos + total_rejeitados) > 0 else 0
    print(f"\n🎉 CONCLUÍDO! | {total_aceitos:,} aceitos ({taxa_geral:.2f}%) | {tempo_total//60}min {tempo_total%60}s")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        sys.exit(1)
