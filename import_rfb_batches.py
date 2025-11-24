#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

ARQUIVO_NUM = int(sys.argv[1]) if len(sys.argv) > 1 else 1
ARQUIVO_NOME = sys.argv[2] if len(sys.argv) > 2 else "Estabelecimentos1.zip"
MES_ANO = sys.argv[3] if len(sys.argv) > 3 else "2024-01"
BATCH_SIZE = 5000  # Inserir 5k registros por vez

# Credenciais Supabase PostgreSQL (Session Pooler)
DB_CONFIG = {
    'host': 'aws-1-sa-east-1.pooler.supabase.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres.oxuqbcltlykvyeaambtq',
    'password': 'IsQ9qbndTR6VNSkI',
    'connect_timeout': 30,
    'sslmode': 'require'
}

BASE_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{MES_ANO}"
URL = f"{BASE_URL}/{ARQUIVO_NOME}"

def main():
    inicio = time.time()
    print("\n" + "="*70)
    print("🚀 RFB ETL - IMPORT COM BATCHES")
    print("="*70)
    print(f"📦 Arquivo: {ARQUIVO_NOME} (#{ARQUIVO_NUM})")
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

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
    csv_file = zip_file.open(zip_file.namelist()[0])
    print(f"      ✅ CSV: {zip_file.namelist()[0]}")
    del zip_data

    # Connect
    print("\n[3/5] 🔌 Conectando PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()
    print("      ✅ Conectado!")

    # Limpar staging
    print("\n[4/5] 🚀 Inserindo em batches...")
    cursor.execute("TRUNCATE estabelecimentos_staging;")
    conn.commit()

    # Ler CSV e inserir em batches
    csv_reader = csv.reader(io.TextIOWrapper(csv_file, encoding='latin-1'), delimiter=';')
    batch = []
    total_registros = 0
    batch_num = 0

    for row in csv_reader:
        # Garantir 29 colunas (preencher vazias se necessário)
        while len(row) < 29:
            row.append('')

        batch.append(tuple(row[:29]))

        if len(batch) >= BATCH_SIZE:
            execute_values(cursor, """
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
                ) VALUES %s
            """, batch)
            conn.commit()

            total_registros += len(batch)
            batch_num += 1

            if batch_num % 100 == 0:
                print(f"      📊 Processados: {total_registros:,} registros ({batch_num} batches)")

            batch = []

    # Inserir últimos registros
    if batch:
        execute_values(cursor, """
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
            ) VALUES %s
        """, batch)
        conn.commit()
        total_registros += len(batch)

    print(f"      ✅ Importado: {total_registros:,} registros")
    csv_file.close()

    # UPSERT
    print("\n[5/5] 💾 UPSERT para tabela final...")
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
    print("\n" + "="*70)
    print("✅ CONCLUÍDO!")
    print("="*70)
    print(f"📊 Registros: {total_registros:,}")
    print(f"⏱️  Tempo: {tempo//60}min {tempo%60}s")
    print("="*70)

    import json
    print(f'\n{json.dumps({"status": "success", "arquivo_numero": ARQUIVO_NUM, "tamanho_mb": round(tamanho_mb, 2), "tempo_segundos": tempo, "registros_staging": total_registros}, ensure_ascii=False)}')

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
