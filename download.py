import requests
import datetime
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import concurrent

def download_file(url, local_filename):
    """
    Downloads a file from a given URL and saves it locally.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size_in_bytes = int(r.headers.get('content-length', 0))
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                progress_bar.update(len(chunk))
                f.write(chunk)
        progress_bar.close()

if __name__ == "__main__":
    sample_weekday = 0 # 0 means monday 1 means tuesdat 2 menas wednesday etc.
    num_downloads_in_parallel = 20

    # copied from https://mawi.wide.ad.jp/mawi/samplepoint-F/2022/
    dates_str = '''2022/01: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    2022/02: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
    2022/03: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    2022/04: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30
    2022/05: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    2022/06: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30
    2022/07: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    2022/08: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    2022/09: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30
    2022/10: 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    2022/11: 01 02 03 04
    2022/12: 14 15 16 17 18 19 20 21 23 24 25 26 27 28 29 30 31'''

    date_list: list[datetime.date] = []

    dates_str = dates_str.replace("\n", "").strip()

    months = dates_str.split("    2022/")[1:]
    for i, m in enumerate(months):
        temp2 = m.split(" ")
        for j, d in enumerate(temp2[1:]):
            int_year = 2022
            int_month = int(temp2[0].replace(":", ""))
            int_day = int(d)
            date_list.append(datetime.date(int_year, int_month, int_day))

    sampledate_list = []
    for date in date_list:
        if date.weekday() != sample_weekday: continue
        sampledate_list.append(date)

    # First see the total size by checking the endpoints? NO
    
    # Just download
    # http://mawi.nezu.wide.ad.jp/mawi/samplepoint-F/2022/2022 01 01 1400.pcap.gz
    
    links = []
    paths = []
    for d in sampledate_list:
        d_str = str(d).replace("-", "")
        the_link = f"http://mawi.nezu.wide.ad.jp/mawi/samplepoint-F/2022/{d_str}1400.pcap.gz"
        the_path = f"./downloads/{d_str}1400.pcap.gz"
        links.append(the_link)
        paths.append(the_path)

    print("Preprocessing complete...")
    print(f"There will be {len(links)} files in total.")

    # Old sequential way
    # for url, path in zip(links, paths):
    #     print(f"Downloading {path} from {url}")
    #     download_file(url, path)
    #     print(f"Downloaded {path}")

    # print("All files have been downloaded.")

    with ThreadPoolExecutor(max_workers=num_downloads_in_parallel) as executor:
        # Start the download operations and mark each future with its URL
        future_to_url = {executor.submit(download_file, url, path): url for url, path in zip(links, paths)}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print(f'{url} generated an exception: {exc}')
            else:
                print(data)

    print("All files have been downloaded.")

    

            
