import shopify
import time
import schedule
import pandas as pd
import sqlite3
import os
from datetime import datetime
from tabulate import tabulate
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Credenciais lidas do arquivo .env
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
shop_url = os.getenv('SHOPIFY_SHOP_URL')
api_version = os.getenv('SHOPIFY_API_VERSION')

# Verificação de credenciais
if not all([access_token, shop_url, api_version]):
    print("ERRO: Credenciais incompletas no arquivo .env")
    print("Por favor, configure as variáveis SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_URL e SHOPIFY_API_VERSION")
    exit(1)

# Configurações
MAPPING_FILE = os.getenv('MAPPING_FILE', 'tipos_produtos_tags.xlsx')  # arquivo inicial para carga
DB_FILE = os.getenv('DB_FILE', 'product_tags.db')  # banco de dados SQLite
TAG_SEPARATOR = ";"  # Define o separador como ponto e vírgula
SHOPIFY_TAG_SEPARATOR = ", "  # Separador usado pelo Shopify para as tags

def setup_session():
    shop_session = shopify.Session(shop_url, api_version, access_token)
    shopify.ShopifyResource.activate_session(shop_session)

def setup_database():
    """Configura o banco de dados SQLite e carrega dados iniciais se necessário"""
    db_exists = os.path.exists(DB_FILE)
    # Conecta ao banco e cria a tabela se não existir
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS product_type_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_type TEXT UNIQUE NOT NULL,
        tags TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    # Se o banco acabou de ser criado, carrega dados da planilha
    if not db_exists:
        print(f"Banco de dados não encontrado. Criando novo banco e importando dados de '{MAPPING_FILE}'...")
        import_from_spreadsheet(conn, cursor)
    else:
        # Verifica se há dados no banco
        cursor.execute("SELECT COUNT(*) FROM product_type_tags")
        count = cursor.fetchone()[0]
        if count == 0:
            print("Banco de dados vazio. Importando dados da planilha...")
            import_from_spreadsheet(conn, cursor)
        else:
            print(f"Banco de dados encontrado com {count} mapeamentos.")
    conn.commit()
    conn.close()

def import_from_spreadsheet(conn, cursor):
    """Importa dados da planilha para o banco SQLite"""
    try:
        # Verifica se o arquivo existe
        if not os.path.exists(MAPPING_FILE):
            print(f"Arquivo '{MAPPING_FILE}' não encontrado. O banco será inicializado vazio.")
            return
        # Carrega a planilha
        if MAPPING_FILE.endswith('.xlsx'):
            df = pd.read_excel(MAPPING_FILE)
        else:  # assume CSV
            df = pd.read_csv(MAPPING_FILE)
        # Verifica as colunas necessárias
        required_columns = ['tipo_produto', 'tags']
        for col in required_columns:
            if col not in df.columns:
                print(f"Erro: A coluna '{col}' não foi encontrada na planilha.")
                return
        # Insere os dados no banco
        for _, row in df.iterrows():
            if pd.notna(row['tipo_produto']) and pd.notna(row['tags']):
                product_type = row['tipo_produto'].strip()
                tags = row['tags'].strip()
                # Evita inserção duplicada
                cursor.execute(
                    "INSERT OR REPLACE INTO product_type_tags (product_type, tags) VALUES (?, ?)",
                    (product_type, tags)
                )
        conn.commit()
        print(f"Dados importados com sucesso da planilha '{MAPPING_FILE}'.")
    except Exception as e:
        print(f"Erro ao importar dados da planilha: {str(e)}")

def load_product_type_mappings():
    """Carrega o mapeamento de tipos de produto do banco SQLite"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT product_type, tags FROM product_type_tags")
        rows = cursor.fetchall()
        mappings = {}
        for product_type, tags_str in rows:
            # Divide as tags por ponto e vírgula e remove espaços em branco
            tags = [tag.strip() for tag in tags_str.split(TAG_SEPARATOR)]
            mappings[product_type] = tags
        conn.close()
        print(f"Carregados {len(mappings)} mapeamentos do banco de dados.")
        return mappings
    except Exception as e:
        print(f"Erro ao carregar mapeamentos do banco: {str(e)}")
        return {}

def list_mappings():
    """Lista todos os mapeamentos de tipo de produto para tags"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT id, product_type, tags, updated_at FROM product_type_tags ORDER BY product_type")
        rows = cursor.fetchall()
        if not rows:
            print("Nenhum mapeamento encontrado no banco de dados.")
            return
        mappings_data = []
        for id, product_type, tags, updated_at in rows:
            mappings_data.append([
                id,
                product_type,
                tags,
                updated_at
            ])
        headers = ["ID", "Tipo de Produto", "Tags", "Última Atualização"]
        print("\nMapeamentos de Tipos de Produto para Tags:")
        print(tabulate(mappings_data,
                     headers=headers,
                     tablefmt="simple",
                     numalign="left"))
        print(f"\nTotal de mapeamentos: {len(rows)}")
        conn.close()
    except Exception as e:
        print(f"Erro ao listar mapeamentos: {str(e)}")

def add_mapping(product_type, tags):
    """Adiciona um novo mapeamento de tipo de produto para tags"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Verifica se o tipo já existe
        cursor.execute("SELECT id FROM product_type_tags WHERE product_type = ?", (product_type,))
        existing = cursor.fetchone()
        if existing:
            update = get_user_confirmation(f"Tipo de produto '{product_type}' já existe. Deseja atualizar? (s/n): ")
            if not update:
                print("Operação cancelada.")
                conn.close()
                return
            cursor.execute(
                "UPDATE product_type_tags SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE product_type = ?",
                (tags, product_type)
            )
            print(f"Mapeamento para '{product_type}' atualizado com sucesso.")
        else:
            cursor.execute(
                "INSERT INTO product_type_tags (product_type, tags) VALUES (?, ?)",
                (product_type, tags)
            )
            print(f"Novo mapeamento para '{product_type}' adicionado com sucesso.")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao adicionar mapeamento: {str(e)}")

def remove_mapping(product_type):
    """Remove um mapeamento de tipo de produto"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM product_type_tags WHERE product_type = ?", (product_type,))
        if cursor.rowcount > 0:
            print(f"Mapeamento para '{product_type}' removido com sucesso.")
        else:
            print(f"Tipo de produto '{product_type}' não encontrado.")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao remover mapeamento: {str(e)}")

def format_tags(tags, max_length=50):
    """Formata lista de tags usando o separador definido"""
    tags_str = TAG_SEPARATOR.join(tags)
    if len(tags_str) > max_length:
        return tags_str[:max_length] + "..."
    return tags_str

def display_products(products_with_type_tags):
    """Exibe os produtos que receberão tags baseadas no tipo"""
    if not products_with_type_tags:
        print("Nenhum produto para atualizar.")
        return
    products_data = []
    for i, (product, tags_to_add) in enumerate(products_with_type_tags, 1):
        current_tags = product.tags.split(SHOPIFY_TAG_SEPARATOR) if product.tags else []
        # Usa o novo formato com ponto e vírgula para exibição
        formatted_current = format_tags(current_tags)
        formatted_to_add = format_tags(tags_to_add)
        products_data.append([
            i,
            product.title[:50],
            product.product_type,
            formatted_current,
            formatted_to_add
        ])
    headers = ["Nº", "Título", "Tipo de Produto", "Tags Atuais", "Tags a Adicionar"]
    print("\nProdutos que receberão tags baseadas no tipo:")
    print(tabulate(products_data,
                 headers=headers,
                 tablefmt="simple",
                 maxcolwidths=[5, 50, 20, 40, 40],
                 numalign="left"))
    print(f"\nTotal de produtos a receber tags: {len(products_with_type_tags)}")

def get_user_confirmation(message):
    while True:
        response = input(message).strip().lower()
        if response in ['s', 'n']:
            return response == 's'
        print("Resposta inválida. Por favor, digite 's' para sim ou 'n' para não.")

def get_products_for_type_tagging(product_type_mappings):
    """Busca produtos que precisam receber tags baseadas no tipo"""
    if not product_type_mappings:
        return []
    products_with_type_tags = []
    limit = 250  # Limite da API por página
    print("Buscando produtos para aplicação de tags por tipo...")
    # Primeira página
    product_page = shopify.Product.find(limit=limit)
    processed_count = 0
    while product_page:
        for product in product_page:
            processed_count += 1
            # Verifica se o tipo de produto está no mapeamento
            product_type = product.product_type
            if product_type in product_type_mappings:
                # Verifica quais tags ainda não estão presentes
                type_tags = product_type_mappings[product_type]
                # O Shopify usa virgula+espaço como separador
                current_tags = set(product.tags.split(SHOPIFY_TAG_SEPARATOR) if product.tags else [])
                tags_to_add = [tag for tag in type_tags if tag not in current_tags]
                if tags_to_add:  # Se há tags para adicionar
                    products_with_type_tags.append((product, tags_to_add))
        # Mostrar progresso
        if processed_count % 250 == 0:
            print(f"Processados {processed_count} produtos...")
        # Verificar se há mais páginas usando paginação baseada em links
        if hasattr(product_page, 'next_page_url') and product_page.next_page_url:
            product_page = product_page.next_page()
        else:
            product_page = None
    print(f"Total de {processed_count} produtos verificados.")
    return products_with_type_tags

def manage_product_type_tags():
    """Gerencia tags baseadas no tipo de produto"""
    try:
        print(f"\n[{datetime.now()}] Iniciando gerenciamento de tags por tipo de produto...")
        setup_session()
        # Carrega mapeamentos do banco
        product_type_mappings = load_product_type_mappings()
        if not product_type_mappings:
            print("Nenhum mapeamento de tipo de produto encontrado no banco de dados.")
            return
        # Busca produtos para gerenciamento de tags
        products_with_type_tags = get_products_for_type_tagging(product_type_mappings)
        if not products_with_type_tags:
            print("Nenhum produto precisa de atualização de tags por tipo.")
            return
        # Exibir produtos encontrados
        display_products(products_with_type_tags)
        # Solicitar confirmação do usuário
        if not get_user_confirmation(f"\nDeseja aplicar estas tags baseadas no tipo para {len(products_with_type_tags)} produtos? (s/n): "):
            print("Operação cancelada pelo usuário")
            return
        # Adicionar tags baseadas no tipo
        update_count = 0
        for product, tags_to_add in products_with_type_tags:
            try:
                # O Shopify usa virgula+espaço como separador
                current_tags = set(product.tags.split(SHOPIFY_TAG_SEPARATOR) if product.tags else [])
                current_tags.update(tags_to_add)
                # Manter o formato do Shopify para salvar as tags
                product.tags = SHOPIFY_TAG_SEPARATOR.join(sorted(current_tags))
                product.save()
                update_count += 1
                print(f"Adicionadas tags para '{product.title}': {TAG_SEPARATOR.join(tags_to_add)}")
            except Exception as e:
                print(f"Erro ao atualizar produto '{product.title}': {str(e)}")
        print(f"\n[{datetime.now()}] Operação concluída!")
        print(f"Produtos atualizados: {update_count}/{len(products_with_type_tags)}")
    except Exception as e:
        print(f"Erro geral: {str(e)}")
    finally:
        shopify.ShopifyResource.clear_session()

def admin_menu():
    """Menu para gerenciamento de mapeamentos"""
    while True:
        print("\n==== GERENCIAMENTO DE MAPEAMENTOS DE TIPOS PARA TAGS ====")
        print("1. Listar todos os mapeamentos")
        print("2. Adicionar ou atualizar mapeamento")
        print("3. Remover mapeamento")
        print("4. Importar mapeamentos da planilha")
        print("5. Executar atualização de tags")
        print("6. Configurar execução automática")
        print("0. Sair")
        choice = input("\nEscolha uma opção: ").strip()
        if choice == '1':
            list_mappings()
        elif choice == '2':
            product_type = input("Digite o tipo de produto: ").strip()
            tags = input(f"Digite as tags separadas por {TAG_SEPARATOR} ").strip()
            if product_type and tags:
                add_mapping(product_type, tags)
            else:
                print("Tipo de produto e tags são obrigatórios.")
        elif choice == '3':
            product_type = input("Digite o tipo de produto a remover: ").strip()
            if product_type:
                confirm = get_user_confirmation(f"Confirma remover o mapeamento para '{product_type}'? (s/n): ")
                if confirm:
                    remove_mapping(product_type)
            else:
                print("Tipo de produto é obrigatório.")
        elif choice == '4':
            confirm = get_user_confirmation(f"Isso importará novos dados da planilha '{MAPPING_FILE}'. Continuar? (s/n): ")
            if confirm:
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                import_from_spreadsheet(conn, cursor)
                conn.close()
        elif choice == '5':
            manage_product_type_tags()
        elif choice == '6':
            configure_auto_execution()
        elif choice == '0':
            print("Saindo do programa...")
            break
        else:
            print("Opção inválida. Tente novamente.")

def configure_auto_execution():
    """Configura execução automática do script"""
    if get_user_confirmation("Deseja configurar a execução automática a cada 6 horas? (s/n): "):
        print("Agendamento configurado. O script será executado a cada 6 horas.")
        print("Pressione CTRL+C para interromper o serviço.")
        # Agenda para executar a cada 6 horas
        schedule.every(6).hours.do(lambda: auto_manage_tags_without_confirmation())
        # Loop para manter o agendamento funcionando
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Verifica a cada minuto
        except KeyboardInterrupt:
            print("\nServiço interrompido pelo usuário.")
    else:
        print("Configuração automática cancelada.")

def auto_manage_tags_without_confirmation():
    """Versão automática sem confirmação para execuções agendadas"""
    try:
        print(f"\n[{datetime.now()}] Iniciando gerenciamento automático de tags por tipo...")
        setup_session()
        # Carrega mapeamentos do banco
        product_type_mappings = load_product_type_mappings()
        if not product_type_mappings:
            print("Nenhum mapeamento de tipo de produto encontrado no banco de dados.")
            return
        # Busca produtos para gerenciamento de tags
        products_with_type_tags = get_products_for_type_tagging(product_type_mappings)
        if not products_with_type_tags:
            print("Nenhum produto precisa de atualização de tags por tipo.")
            return
        print(f"Encontrados {len(products_with_type_tags)} produtos para receber tags baseadas no tipo.")
        # Adicionar tags baseadas no tipo
        update_count = 0
        for product, tags_to_add in products_with_type_tags:
            try:
                # O Shopify usa virgula+espaço como separador
                current_tags = set(product.tags.split(SHOPIFY_TAG_SEPARATOR) if product.tags else [])
                current_tags.update(tags_to_add)
                # Manter o formato do Shopify para salvar as tags
                product.tags = SHOPIFY_TAG_SEPARATOR.join(sorted(current_tags))
                product.save()
                update_count += 1
            except Exception as e:
                print(f"Erro ao atualizar produto '{product.title}': {str(e)}")
        print(f"[{datetime.now()}] Execução automática concluída!")
        print(f"Produtos atualizados: {update_count}/{len(products_with_type_tags)}")
    except Exception as e:
        print(f"Erro na execução automática: {str(e)}")
    finally:
        shopify.ShopifyResource.clear_session()

if __name__ == "__main__":
    # Configura o banco de dados
    setup_database()
    # Exibe o menu de administração
    admin_menu()
