import requests
import zipfile
import io
import os

# Публичная ссылка на zip-файл
yadisk_url = "https://disk.yandex.ru/d/XPthmNk_pqEDaQ"

# Получаем прямую ссылку для скачивания
response = requests.get(f"https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key={yadisk_url}")
download_info = response.json()
download_url = download_info['href']

# Скачиваем zip-файл в память
r = requests.get(download_url)
z = zipfile.ZipFile(io.BytesIO(r.content))

# Создаем папку для распаковки
os.makedirs("datasets", exist_ok=True)

# Распаковываем все файлы
z.extractall("datasets")

print("✅ Датасеты загружены и распакованы в папку 'datasets'")
