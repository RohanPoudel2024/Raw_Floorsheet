import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from datetime import datetime, timedelta
import os

def get_hidden_fields(soup):
    fields = {}
    for hidden in soup.find_all('input', type='hidden'):
        if hidden.get('name'):
            fields[hidden.get('name')] = hidden.get('value')
    return fields

def scrape_floorsheet(start_date_str, end_date_str, output_dir="data"):
    url = "https://merolagani.com/Floorsheet.aspx"
    
    start_date = datetime.strptime(start_date_str, "%m/%d/%Y")
    end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
    
    delta = timedelta(days=1)
    current_date = start_date
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    session = requests.Session()
    # Initial GET to load the page and get viewstate
    print("Loading initial page...")
    try:
        response = session.get(url, timeout=20)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Failed to load initial page: {e}")
        return

    while current_date <= end_date:
        # Note: Sunday to Thursday are trading days usually, but we can just check all days.
        # If no table or no rows, we just skip.
        date_str = current_date.strftime("%m/%d/%Y")
        print(f"\n--- Scraping for Date: {date_str} ---")
        
        # Prepare search POST
        data = get_hidden_fields(soup)
        data['ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter'] = date_str
        data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet'
        data['__EVENTARGUMENT'] = ''
        
        try:
            response = session.post(url, data=data, timeout=20)
            soup = BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Error fetching data for {date_str}: {e}")
            current_date += delta
            continue

        table = soup.find('table', {'class': 'table table-bordered table-striped table-hover sortable'})
        if not table:
            print(f"No data found for {date_str}")
            current_date += delta
            continue
            
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')[1:]
        if not rows or (len(rows) == 1 and "No Record Found" in rows[0].text):
             print(f"No record found for {date_str}")
             current_date += delta
             continue
             
        # Determine total pages
        total_pages = 1
        last_page_link = soup.find('a', title='Last Page')
        if last_page_link:
            onclick = last_page_link.get('onclick', '')
            match = re.search(r'changePageIndex\("(\d+)"', onclick)
            if match:
                total_pages = int(match.group(1))
        
        print(f"Found {total_pages} pages for {date_str}.")
        
        # Determine the correct CSV file for this month
        month_str = current_date.strftime("%Y_%m")
        output_csv = os.path.join(output_dir, f"{month_str}_floorsheet.csv")
        
        file_exists = os.path.isfile(output_csv)
        
        # Open CSV file to append
        with open(output_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Date', 'S.N.', 'Transact. No.', 'Symbol', 'Buyer', 'Seller', 'Quantity', 'Rate', 'Amount'])
            
            # Scrape page 1
            print(f"Scraping page 1 / {total_pages}...")
            for row in rows:
                cols = [td.text.strip() for td in row.find_all('td')]
                if len(cols) == 8: # Expecting 8 columns
                    writer.writerow([date_str] + cols)
            
            # Loop through subsequent pages
            for page in range(2, total_pages + 1):
                time.sleep(1) # Be polite
                print(f"Scraping page {page} / {total_pages}...")
                
                data = get_hidden_fields(soup)
                data['ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter'] = date_str
                data['ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage'] = str(page)
                data['ctl00$ContentPlaceHolder1$PagerControl1$btnPaging'] = ''
                data['__EVENTTARGET'] = ''
                data['__EVENTARGUMENT'] = ''
                
                try:
                    page_response = session.post(url, data=data, timeout=20)
                    soup = BeautifulSoup(page_response.content, 'html.parser')
                except Exception as e:
                    print(f"Error on page {page}: {e}")
                    break # Skip the rest of pages for this date if it fails repeatedly
                
                table = soup.find('table', {'class': 'table table-bordered table-striped table-hover sortable'})
                if not table:
                    print(f"Warning: Table not found on page {page}. Exiting pagination for {date_str}.")
                    break
                    
                tbody = table.find('tbody')
                rows = tbody.find_all('tr') if tbody else table.find_all('tr')[1:]
                
                for row in rows:
                    cols = [td.text.strip() for td in row.find_all('td')]
                    if len(cols) == 8:
                        writer.writerow([date_str] + cols)

        print(f"Finished {date_str}.")
        current_date += delta
        time.sleep(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Merolagani Floorsheet Data")
    parser.add_argument("--start", type=str, required=True, help="Start date (MM/DD/YYYY)")
    parser.add_argument("--end", type=str, required=True, help="End date (MM/DD/YYYY)")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory for monthly CSVs")
    args = parser.parse_args()
    
    scrape_floorsheet(args.start, args.end, args.output_dir)
