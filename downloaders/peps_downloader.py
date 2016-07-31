import argparse
import requests
import datetime
import sys
import os

parser = argparse.ArgumentParser()

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('-c', '--collection', help='Sentinel collection available in THEIA', choices=['S1', 'S2', 'S3'], default='S2')
parser.add_argument('-s', '--start_date', help='Start date in format YYYY-MM-DD', required=True)
parser.add_argument('-e', '--end_date', help='End date in format YYYY-MM-DD' ,default=datetime.date.today().isoformat())
parser.add_argument('-a', '--auth', help='credentials file ', default='auth.txt')
parser.add_argument('-o', '--orbit', help='Orbit Path number', type=int)
parser.add_argument('-n', '--no_download', help='Does not start the download, prints the search results only', action='store_true', default=False)
parser.add_argument('-p', '--output_directory', help='Target directory path for the downloaded products', default='.')

subparsers = parser.add_subparsers(dest="cmd", help='The way to specify search target')

location_parser = subparsers.add_parser('location', help='City or town with argument e.g.: location -l Gdansk')
location_parser.add_argument('-l', '--location', type=str, required=True, help='location to use as a keyword')

point_parser = subparsers.add_parser('point', help='point given with --lon and --lat')
point_parser.add_argument('--lon', type=str, required=True, help='Longitude in decimal degrees')
point_parser.add_argument('--lat', type=str, required=True, help='Latitude in decimal degrees')

rectangle_parser = subparsers.add_parser('rectangle', help='rectangle given with longitude and latitude (--lonmin --lonmax --latmin --latmax)')
rectangle_parser.add_argument('--lonmin', type=str, required=True, help='minimal longitude in decimal degrees')
rectangle_parser.add_argument('--lonmax', type=str, required=True, help='maximal longitude in decimal degrees')
rectangle_parser.add_argument('--latmin', type=str, required=True, help='minimal latitude in decimal degrees')
rectangle_parser.add_argument('--latmax', type=str, required=True, help='maximal latitude in decimal degrees')

args = parser.parse_args()


if len(sys.argv) == 1:
    parser.print_help()
    parser.exit()

def get_target():
    strategy = args.cmd
    if strategy == "rectangle":
        return "box={},{},{},{}".format(args.latmin, args.latmax, args.lonmin, args.lonmax)
    elif strategy == "location":
        return "q={}".format(args.location)
    elif strategy == "point":
        return "lat={}&lon={}".format(args.lat, args.lon)


def get_auth(auth_filepath='auth.txt'):
    with open(auth_filepath, 'r') as f:
        creds = f.readline().split()
        if len(creds) != 2:
            raise IOError("Malformed credentials file. Should contain 'username password' only")
        return tuple(creds)

def search_products():
    request_template = "https://peps.cnes.fr/resto/api/collections/{collection}/search.json?{location}&startDate={start_date}&completionDate={end_date}&maxRecords=500"
    r = request_template.format(collection=args.collection, location=get_target(), start_date=args.start_date,
                                end_date=args.end_date)
    print r
    results = requests.get(r, auth=get_auth()).json()
    return results['features']

def filter_products(products):
    filtered = products
    if args.orbit:
        filtered = [product for product in products if "_R{0:03d}".format(args.orbit) in product['properties']['productIdentifier']]
    return filtered

def get_download_request_url(id):
   request_template = "https://peps.cnes.fr/resto/collections/{collection}/{id}/download/?issuerId=peps"
   return request_template.format(collection=args.collection, id=id)

def size_of(num, suffix='B'):
    prefixes = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']
    for prefix in prefixes[:-1]:
        if abs(num) < 1024.0:
            return "{:3.1f}{}{}".format(num, prefix, suffix)
        num /= 1024.0
    return "{:3.1f}{}{}".format(num, prefixes[-1], suffix) # for last prefix on the list

def download_with_curl(id, product_identifier):
    output_file = os.path.join(args.output_directory, product_identifier + ".zip")
    username, password = get_auth()
    curl_download_template = 'curl -o {output_file} -k -u "{username}":"{password}" https://peps.cnes.fr/resto/collections/{collection}/{id}/download/?issuerId=peps'
    curl_download = curl_download_template.format(output_file=output_file,
                                                  username=username,
                                                  password=password,
                                                  collection=args.collection,
                                                  id=id)
    print curl_download
    os.system(curl_download)


def save_from_url_with_progress_bar(id, product_identifier):
    request_url = get_download_request_url(id)
    with open(os.path.join(args.output_directory, product_identifier + ".zip"), "wb") as f:
        print "Downloading Sentinel product {}".format(product_identifier)
        response = requests.get(request_url, stream=True, auth=get_auth())
        total_length = response.headers.get('content-length')

        if total_length is None:  # no content length header
            f.write(response.content)
        else:
            dl = 0
            total_length = int(total_length)
            for data in response.iter_content(chunk_size=10000):
                dl += len(data)
                f.write(data)
                done = int(50 * dl / total_length)
                sys.stdout.write("\r[{}{}] {:6s}/{:6s}".format('=' * done, ' ' * (50 - done), size_of(dl),
                                                               size_of(total_length)))
                sys.stdout.flush()


results = filter_products(search_products())

for result in results:
    id = result['id']
    product_id = result['properties']['productIdentifier']
    print product_id
    if not args.no_download:
        save_from_url_with_progress_bar(id, product_id)