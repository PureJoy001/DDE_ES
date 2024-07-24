import json
import geopandas as gpd
from shapely.geometry import Polygon


def find_continents(lat1, lon1, lat2, lon2):
    with open('utils/continent.geojson') as f:
        continents = json.load(f).get('features', [])
    border_data = [continent['geometry']['coordinates'] for continent in continents]
    result = [False] * 8

    coords1 = [(lat1, lon1), (lat2, lon2), (lat1, lon2), (lat2, lon1)]
    poly1 = Polygon(coords1)
    gs1 = gpd.GeoSeries([poly1], crs='EPSG:4326')

    for i, continent in enumerate(border_data):
        for district in continent:
            coords2 = district[0]
            poly2 = Polygon(coords2)
            gs2 = gpd.GeoSeries([poly2], crs='EPSG:4326')
            if gs1.intersects(gs2)[0]:
                result[i] = True
                break
    result[5] = result[5] or result[6]
    result[6] = result[7]
    result = result[:7]
    return [i for i, value in enumerate(result) if value]


if __name__ == '__main__':
    print(find_continents(-48.71722, 80.513337, 104.19433, 75.157011))
    print(find_continents(-48.71722, 75.157011, 104.19433,80.513337 ))
    #print(find_continents(-180, -90, 180, 90))

    print(find_continents(-17.80000000044613, 38.49999999958506, 61.89999999978436, -35.8000000002402))