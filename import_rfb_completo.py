#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RFB ETL COMPLETO - Processa TODOS os arquivos (0 a 9) SEM FILTRO de CNAE
- Batch size: 10.000 registros
- Usa execute_values() (3-5x mais rápido)
- Staging + UPSERT
- TODOS os 25 milhões de estabelecimentos
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
BATCH_SIZE = 10000

# Connection String (Transaction Pooler - Porta 6543)
CONN_STRING = "postgresql://postgres.oxuqbcltlykvyeaambtq:IsQ9qbndTR6VNSkI@aws-1-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require"

BASE_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{MES_ANO}"

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

def processar_arquivo(arquivo_num):
    """Processa um único arquivo"""
    arquivo_nome = f"Estabelecimentos{arquivo_num}.zip"
    url = f"{BASE_URL}/{arquivo_nome}"

    inicio = time.time()
    print("\n" + "="*70)
    print(f"📦 Arquivo: {arquivo_nome} (#{arquivo_num})")
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Download
    print("\n[1/4] 📥 Baixando arquivo...")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    response = urllib.request.urlopen(req, timeout=600)
    zip_data = response.read()
    tamanho_mb = len(zip_data) / (1024 * 1024)
    print(f"      ✅ Download: {tamanho_mb:.2f}MB")

    # Unzip
    print("\n[2/4] 📦 Descompactando...")
    zip_file = zipfile.ZipFile(io.BytesIO(zip_data))
    csv_filename = zip_file.namelist()[0]
    csv_file = io.TextIOWrapper(zip_file.open(csv_filename), encoding='latin-1')
    print(f"      ✅ CSV: {csv_filename}")
    del zip_data

    # Connect
    print("\n[3/4] 🔌 Conectando PostgreSQL...")
    conn = conectar_db()
    cursor = conn.cursor()
    print("      ✅ Conectado!")

    # Limpar staging
    print("\n[4/4] 🚀 Inserindo na STAGING (SEM FILTRO)...")
    cursor.execute("TRUNCATE estabelecimentos_staging;")
    conn.commit()

    # Ler CSV e inserir em batches (SEM FILTRO)
    csv_reader = csv.reader(csv_file, delimiter=';', quotechar='"')
    batch = []
    total_staging = 0
    batch_num = 0

    for row in csv_reader:
        # Remover aspas extras
        row = [campo.strip('"') if campo else '' for campo in row]

        # Garantir 30 colunas
        while len(row) < 30:
            row.append('')

        batch.append(tuple(row[:30]))

        if len(batch) >= BATCH_SIZE:
            execute_values(
                cursor,
                """
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
                """,
                batch,
                page_size=1000
            )
            conn.commit()

            total_staging += len(batch)
            batch_num += 1

            if batch_num % 5 == 0:
                print(f"      📊 Staging: {total_staging:,} registros")

            batch = []

    # Último batch
    if batch:
        execute_values(
            cursor,
            """
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
            """,
            batch,
            page_size=1000
        )
        conn.commit()
        total_staging += len(batch)

    print(f"      ✅ Staging: {total_staging:,} registros")
    csv_file.close()

    # UPSERT
    print("\n[5/5] 💾 UPSERT staging → estabelecimentos...")
    cursor.execute("""
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
        )
        SELECT
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
        FROM estabelecimentos_staging
        ON CONFLICT (cnpj_completo) DO UPDATE SET
          identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
          nome_fantasia = EXCLUDED.nome_fantasia,
          situacao_cadastral = EXCLUDED.situacao_cadastral,
          data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
          motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
          nome_cidade_exterior = EXCLUDED.nome_cidade_exterior,
          pais = EXCLUDED.pais,
          data_inicio_atividade = EXCLUDED.data_inicio_atividade,
          cnae_fiscal_principal = EXCLUDED.cnae_fiscal_principal,
          cnae_fiscal_secundaria = EXCLUDED.cnae_fiscal_secundaria,
          tipo_logradouro = EXCLUDED.tipo_logradouro,
          logradouro = EXCLUDED.logradouro,
          numero = EXCLUDED.numero,
          complemento = EXCLUDED.complemento,
          bairro = EXCLUDED.bairro,
          cep = EXCLUDED.cep,
          uf = EXCLUDED.uf,
          municipio = EXCLUDED.municipio,
          ddd_1 = EXCLUDED.ddd_1,
          telefone_1 = EXCLUDED.telefone_1,
          ddd_2 = EXCLUDED.ddd_2,
          telefone_2 = EXCLUDED.telefone_2,
          ddd_fax = EXCLUDED.ddd_fax,
          fax = EXCLUDED.fax,
          correio_eletronico = EXCLUDED.correio_eletronico,
          situacao_especial = EXCLUDED.situacao_especial,
          data_situacao_especial = EXCLUDED.data_situacao_especial,
          updated_at = NOW();
    """)
    conn.commit()
    print("      ✅ UPSERT completo!")

    cursor.close()
    conn.close()

    tempo = int(time.time() - inicio)

    print("\n" + "="*70)
    print(f"✅ Arquivo {arquivo_num} CONCLUÍDO!")
    print("="*70)
    print(f"📊 Registros: {total_staging:,}")
    print(f"⏱️  Tempo: {tempo//60}min {tempo%60}s")
    print(f"⚡ Performance: {total_staging/tempo:.0f} registros/segundo")
    print("="*70)

    return {
        "arquivo": arquivo_num,
        "registros": total_staging,
        "tempo": tempo,
        "tamanho_mb": round(tamanho_mb, 2)
    }

def main():
    inicio_total = time.time()

    print("\n" + "="*70)
    print("🚀 RFB ETL COMPLETO - TODOS OS ARQUIVOS (0-9) SEM FILTRO")
    print("="*70)
    print(f"📅 Mês/Ano: {MES_ANO}")
    print(f"⚡ Batch: {BATCH_SIZE:,} registros")
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    resultados = []
    total_registros = 0

    # Processar arquivos 0 a 9
    for i in range(10):
        try:
            resultado = processar_arquivo(i)
            resultados.append(resultado)
            total_registros += resultado["registros"]
        except Exception as e:
            print(f"\n❌ ERRO no arquivo {i}: {e}")
            import traceback
            traceback.print_exc()
            continue

    tempo_total = int(time.time() - inicio_total)

    # Resumo final
    print("\n\n" + "="*70)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print("="*70)
    print(f"📦 Arquivos processados: {len(resultados)}/10")
    print(f"📊 Total de registros: {total_registros:,}")
    print(f"⏱️  Tempo total: {tempo_total//60}min {tempo_total%60}s")
    print(f"⚡ Performance média: {total_registros/tempo_total:.0f} registros/segundo")
    print("="*70)

    # Detalhamento por arquivo
    print("\n📋 DETALHAMENTO POR ARQUIVO:")
    print("-"*70)
    for r in resultados:
        print(f"  Arquivo {r['arquivo']}: {r['registros']:,} registros | {r['tempo']//60}min {r['tempo']%60}s | {r['tamanho_mb']}MB")
    print("="*70)

    import json
    print(f'\n{json.dumps({"status": "success", "total_arquivos": len(resultados), "total_registros": total_registros, "tempo_total_segundos": tempo_total, "resultados": resultados}, ensure_ascii=False)}')

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n❌ ERRO GERAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
