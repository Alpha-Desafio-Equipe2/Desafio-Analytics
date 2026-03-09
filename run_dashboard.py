import os
import subprocess
import sys

def run():
    print("🚀 Iniciando Alpha Analytics Dashboard...")
    
    # Garantir que estamos no diretório correto
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    
    # Comando para rodar o streamlit usando o módulo python para evitar erros de PATH
    app_path = os.path.join("src", "b3_analytics", "app", "main.py")
    
    try:
        # Usa sys.executable para garantir que o Streamlit rode no mesmo ambiente virtual
        subprocess.run([sys.executable, "-m", "streamlit", "run", app_path], check=True)
    except KeyboardInterrupt:
        print("\n👋 Dashboard encerrado.")
    except Exception as e:
        print(f"❌ Erro ao iniciar Dashboard: {e}")

if __name__ == "__main__":
    run()
