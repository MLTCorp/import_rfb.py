#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, urllib.request, zipfile, io, time, csv
from datetime import datetime

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2 import sql

ARQUIVO_NUM = int(sys.argv[1]) if len(sys.argv) > 1 else 1
ARQUIVO_NOME = sys.argv[2] if len(sys.argv) > 2 else "Estabelecimentos1.zip"
MES_ANO = sys.argv[3] if len(sys.argv) > 3 else "2024-01"
CSV_LOCAL = sys.argv[4] if len(sys.argv) > 4 else None
BATCH_SIZE = 1000

# LISTA DE CNAEs PERMITIDOS
CNAES_PERMITIDOS = {
    '4744099', '4744005', '4679699', '4674500', '2330302', '2330399', '4744001',
    '4742300', '4743100', '4744002', '4741500', '4744004', '4744003', '4744006'
}

# Connection String Completa (Transaction Pooler)
# Formato: postgresql://user:password@host:port/database?sslmode=require
CONN_STRING = "postgresql://postgres.oxuqbcltlykvyeaambtq:IsQ9qbndTR6VNSkI@aws-1-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require"

BASE_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{MES_ANO}"
URL = f"{BASE_URL}/{ARQUIVO_NOME}"

def conectar_db(tentativas=3):
    """Tenta conectar ao banco com retry"""
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

def main():
    inicio = time.time()
    print("\n" + "="*70)
    print("🚀 RFB ETL - IMPORT COM STAGING + UPSERT (14 CNAEs)")
    print("="*70)
    print(f"📦 Arquivo: {ARQUIVO_NOME} (#{ARQUIVO_NUM})")
    print(f"🎯 Filtro: {len(CNAES_PERMITIDOS)} CNAEs permitidos")
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    tamanho_mb = 0

    # Verificar se tem CSV local ou precisa baixar
    if CSV_LOCAL and CSV_LOCAL != "None":
        print(f"\n[1/5] 📂 Usando arquivo local: {CSV_LOCAL}")
        csv_file = open(CSV_LOCAL, 'r', encoding='latin-1')
    else:
        # Download
        print("\n[1/5] 📥 Baixando arquivo...")
        req = urllib.request.Request(URL, headers={'User-Agent': 'Mozilla/5.0'})
        response = urllib.request.urlopen(req, timeout=600)
        zip_data = response.read()
        tamanho_mb = len(zip_data) / (1024 * 1024)
        print(f"      ✅ Download: {tamanho_mb:.2f}MB")

        # Unzip
        print("\n[2/5] 📦 Descompactando...")
        zip_file = zipfile.ZipFile(io.BytesIO(zip_data))
        csv_filename = zip_file.namelist()[0]
        csv_file = io.TextIOWrapper(zip_file.open(csv_filename), encoding='latin-1')
        print(f"      ✅ CSV: {csv_filename}")
        del zip_data

    # Connect
    print("\n[3/5] 🔌 Conectando PostgreSQL...")
    conn = conectar_db()
    cursor = conn.cursor()
    print("      ✅ Conectado!")

    # Limpar staging
    print("\n[4/5] 🚀 Filtrando e inserindo na STAGING...")
    cursor.execute("TRUNCATE estabelecimentos_staging;")
    conn.commit()

    # Preparar INSERT para staging
    insert_sql = """
        INSERT INTO estabelecimentos_staging (
          cnpj_basico, cnpj_ordem, cnpj_dv,
          identificador_matriz_filial, nome_fantasia,
          situacao_cadastral, data_situacao_cadastral,
          motivo_situacao_cadastral, nome_cidade_exterior,
          pais, data_inicio_atividade,
          cnae_fiscal_principal, cnae_fiscal_secundaria,
          tipo_logradouro, logradouro, numero, complemento, bairro,
          cep, uf, municipio,
          ddd_1, telefone_1, ddd_2, telefone_2,
          ddd_fax, fax, correio_eletronico,
          situacao_especial, data_situacao_especial
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    # Ler CSV e inserir em batches (COM FILTRO)
    csv_reader = csv.reader(csv_file, delimiter=';')
    batch = []
    total_staging = 0
    total_descartados = 0
    batch_num = 0

    for row in csv_reader:
        # Filtro CNAE (coluna 11)
        cnae = row[11] if len(row) > 11 else ''

        if cnae not in CNAES_PERMITIDOS:
            total_descartados += 1
            continue

        # Garantir 29 colunas
        while len(row) < 29:
            row.append('')

        batch.append(row[:29])

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_sql, batch)
            conn.commit()

            total_staging += len(batch)
            batch_num += 1

            if batch_num % 10 == 0:
                print(f"      📊 Staging: {total_staging:,} | Descartados: {total_descartados:,}")

            batch = []

    # Último batch
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        total_staging += len(batch)

    print(f"      ✅ Staging: {total_staging:,} registros")
    print(f"      🗑️  Descartados: {total_descartados:,}")
    csv_file.close()

    # UPSERT
    print("\n[5/5] 💾 UPSERT staging → estabelecimentos...")
    cursor.execute("""
        INSERT INTO estabelecimentos (
          cnpj_basico, cnpj_ordem, cnpj_dv,
          identificador_matriz_filial, nome_fantasia,
          situacao_cadastral, data_situacao_cadastral,
          cnae_fiscal_principal, cnae_fiscal_secundaria,
          logradouro, cep, uf, municipio,
          ddd_1, telefone_1, ddd_2, telefone_2
        )
        SELECT
          cnpj_basico, cnpj_ordem, cnpj_dv,
          identificador_matriz_filial, nome_fantasia,
          situacao_cadastral, data_situacao_cadastral,
          cnae_fiscal_principal, cnae_fiscal_secundaria,
          logradouro, cep, uf, municipio,
          ddd_1, telefone_1, ddd_2, telefone_2
        FROM estabelecimentos_staging
        ON CONFLICT (cnpj_completo) DO UPDATE SET
          identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
          nome_fantasia = EXCLUDED.nome_fantasia,
          situacao_cadastral = EXCLUDED.situacao_cadastral,
          data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
          cnae_fiscal_principal = EXCLUDED.cnae_fiscal_principal,
          cnae_fiscal_secundaria = EXCLUDED.cnae_fiscal_secundaria,
          logradouro = EXCLUDED.logradouro,
          cep = EXCLUDED.cep,
          uf = EXCLUDED.uf,
          municipio = EXCLUDED.municipio,
          ddd_1 = EXCLUDED.ddd_1,
          telefone_1 = EXCLUDED.telefone_1,
          ddd_2 = EXCLUDED.ddd_2,
          telefone_2 = EXCLUDED.telefone_2,
          updated_at = NOW();
    """)
    conn.commit()
    print("      ✅ UPSERT completo!")

    cursor.close()
    conn.close()

    tempo = int(time.time() - inicio)
    taxa = (total_staging / (total_staging + total_descartados)) * 100 if (total_staging + total_descartados) > 0 else 0

    print("\n" + "="*70)
    print("✅ CONCLUÍDO!")
    print("="*70)
    print(f"📊 Staging: {total_staging:,}")
    print(f"🗑️  Descartados: {total_descartados:,}")
    print(f"📈 Taxa: {taxa:.2f}%")
    print(f"⏱️  Tempo: {tempo//60}min {tempo%60}s")
    print("="*70)

    import json
    print(f'\n{json.dumps({"status": "success", "arquivo_numero": ARQUIVO_NUM, "tamanho_mb": round(tamanho_mb, 2), "tempo_segundos": tempo, "registros_staging": total_staging, "registros_descartados": total_descartados}, ensure_ascii=False)}')

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
