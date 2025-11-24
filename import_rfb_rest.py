#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, urllib.request, zipfile, io, time, csv, json
from datetime import datetime

ARQUIVO_NUM = int(sys.argv[1]) if len(sys.argv) > 1 else 1
ARQUIVO_NOME = sys.argv[2] if len(sys.argv) > 2 else "Estabelecimentos1.zip"
MES_ANO = sys.argv[3] if len(sys.argv) > 3 else "2024-01"
CSV_LOCAL = sys.argv[4] if len(sys.argv) > 4 else None  # Caminho para CSV local (opcional)

BATCH_SIZE = 1000  # REST API aguenta bem

# LISTA DE CNAEs PERMITIDOS
CNAES_PERMITIDOS = {
    '4744099', '4744005', '4679699', '4674500', '2330302', '2330399', '4744001',
    '4742300', '4743100', '4744002', '4741500', '4744004', '4744003', '4744006'
}

# Credenciais Supabase REST API
SUPABASE_URL = "https://oxuqbcltlykvyeaambtq.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im94dXFiY2x0bHlrdnllYWFtYnRxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjMzODIzNzMsImV4cCI6MjA3ODk1ODM3M30.VSr6aFcGkB2lfjRRv6iA3aKuCfyc8ElI82bWNCsUXnQ"

BASE_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{MES_ANO}"
URL = f"{BASE_URL}/{ARQUIVO_NOME}"

def inserir_batch_api(batch):
    """Insere batch via REST API do Supabase"""
    url = f"{SUPABASE_URL}/rest/v1/estabelecimentos"
    headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }

    data = json.dumps(batch).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')

    try:
        response = urllib.request.urlopen(req, timeout=30)
        return True
    except Exception as e:
        print(f"      ⚠️ Erro API: {str(e)[:80]}")
        return False

def main():
    inicio = time.time()
    print("\n" + "="*70)
    print("🚀 RFB ETL - IMPORT VIA REST API (14 CNAEs)")
    print("="*70)
    print(f"📦 Arquivo: {ARQUIVO_NOME} (#{ARQUIVO_NUM})")
    print(f"🎯 Filtro: {len(CNAES_PERMITIDOS)} CNAEs permitidos")
    print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    tamanho_mb = 0

    # Verificar se tem CSV local ou precisa baixar
    if CSV_LOCAL and CSV_LOCAL != "None":
        print(f"\n[1/4] 📂 Usando arquivo local: {CSV_LOCAL}")
        csv_file = open(CSV_LOCAL, 'r', encoding='latin-1')
    else:
        # Download
        print("\n[1/4] 📥 Baixando arquivo...")
        req = urllib.request.Request(URL, headers={'User-Agent': 'Mozilla/5.0'})
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

    # Processar CSV
    print("\n[3/4] 🚀 Filtrando e enviando via REST API...")
    csv_reader = csv.reader(csv_file, delimiter=';')

    batch = []
    total_inseridos = 0
    total_descartados = 0
    total_erros = 0
    batch_num = 0

    for row in csv_reader:
        # Filtro CNAE (coluna 11)
        cnae = row[11] if len(row) > 11 else ''

        if cnae not in CNAES_PERMITIDOS:
            total_descartados += 1
            continue

        # Montar objeto para API (formato JSON)
        cnpj_completo = (row[0] if len(row) > 0 else '') + (row[1] if len(row) > 1 else '') + (row[2] if len(row) > 2 else '')

        estabelecimento = {
            'cnpj_basico': row[0] if len(row) > 0 else '',
            'cnpj_ordem': row[1] if len(row) > 1 else '',
            'cnpj_dv': row[2] if len(row) > 2 else '',
            'identificador_matriz_filial': row[3] if len(row) > 3 and row[3] else None,
            'nome_fantasia': row[4] if len(row) > 4 and row[4] else None,
            'situacao_cadastral': row[5] if len(row) > 5 and row[5] else None,
            'data_situacao_cadastral': row[6] if len(row) > 6 and row[6] else None,
            'cnae_fiscal_principal': row[11] if len(row) > 11 and row[11] else None,
            'cnae_fiscal_secundaria': row[12] if len(row) > 12 and row[12] else None,
            'logradouro': row[14] if len(row) > 14 and row[14] else None,
            'cep': row[18] if len(row) > 18 and row[18] else None,
            'uf': row[19] if len(row) > 19 and row[19] else None,
            'municipio': row[20] if len(row) > 20 and row[20] else None,
            'ddd_1': row[21] if len(row) > 21 and row[21] else None,
            'telefone_1': row[22] if len(row) > 22 and row[22] else None,
            'ddd_2': row[23] if len(row) > 23 and row[23] else None,
            'telefone_2': row[24] if len(row) > 24 and row[24] else None
        }

        batch.append(estabelecimento)

        # Enviar batch
        if len(batch) >= BATCH_SIZE:
            if inserir_batch_api(batch):
                total_inseridos += len(batch)
            else:
                total_erros += len(batch)

            batch_num += 1

            if batch_num % 5 == 0:
                print(f"      📊 Inseridos: {total_inseridos:,} | Descartados: {total_descartados:,} | Erros: {total_erros}")

            batch = []
            time.sleep(0.05)  # Rate limiting

    # Último batch
    if batch:
        if inserir_batch_api(batch):
            total_inseridos += len(batch)
        else:
            total_erros += len(batch)

    csv_file.close()

    print(f"      ✅ Inseridos: {total_inseridos:,}")
    print(f"      🗑️  Descartados: {total_descartados:,}")
    if total_erros > 0:
        print(f"      ⚠️  Erros: {total_erros}")

    tempo = int(time.time() - inicio)
    taxa = (total_inseridos / (total_inseridos + total_descartados)) * 100 if (total_inseridos + total_descartados) > 0 else 0

    print("\n" + "="*70)
    print("✅ CONCLUÍDO!")
    print("="*70)
    print(f"📊 Inseridos: {total_inseridos:,}")
    print(f"🗑️  Descartados: {total_descartados:,}")
    print(f"📈 Taxa: {taxa:.2f}%")
    print(f"⏱️  Tempo: {tempo//60}min {tempo%60}s")
    print("="*70)

    print(f'\n{json.dumps({"status": "success", "arquivo_numero": ARQUIVO_NUM, "tamanho_mb": round(tamanho_mb, 2), "tempo_segundos": tempo, "registros_inseridos": total_inseridos, "registros_descartados": total_descartados}, ensure_ascii=False)}')

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
