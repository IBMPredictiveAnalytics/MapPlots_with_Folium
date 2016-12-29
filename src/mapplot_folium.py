import folium
import pandas as pd
import sys
import os

# convert colors from a string of the form r=X,g=Y,b=Z to #RRGGBB notation
def convertColor(s):
    [r,g,b] = s.split(",")
    [_,r] = r.split("=")
    [_,g] = g.split("=")
    [_,b] = b.split("=")
    return "#%02X%02X%02X"%(int(r),int(g),int(b))

if len(sys.argv) > 1 and sys.argv[1] == "-test":
    df = pd.read_csv("example/airports.csv")
    label_field = "AirportName"
    lat_field = "Latitude"
    lon_field = "Longitude"
    marker_type = "circle" # simple
    circle_radius_type = "radius_fixed"
    circle_radius = 5000
    circle_radius_field = ""
    circle_color = convertColor("r=239,g=51,b=56")
    circle_outline_color = convertColor("r=0,g=255,b=56")
    output_option = 'output_to_screen'
    output_path = ''
    output_width = 1024
    output_height = 1024
    viewer_command = "firefox"
    tile_type = "Stamen Terrain" # "OpenStreamMap" "Stamen Toner"
    title = "Airports of Finland"
else:
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    sqlCtx = ascontext.getSparkSQLContext()
    df = ascontext.getSparkInputData().toPandas()
    label_field = '%%label_field%%'
    lat_field = '%%lat_field%%'
    lon_field = '%%lon_field%%'
    marker_type = '%%marker_type%%'
    circle_radius_type = '%%circle_radius_type%%'
    circle_radius = int('%%circle_radius%%')
    circle_radius_field = '%%circle_radius_field%%'
    circle_color = convertColor('%%circle_color%%')
    circle_outline_color = convertColor('%%circle_outline_color%%')
    output_option = '%%output_option%%'
    output_path = '%%output_path%%'
    output_width = int('%%output_width%%')
    output_height = int('%%output_height%%')
    viewer_command = '%%viewer_command%%'
    tile_type = '%%tile_type%%'
    title = '%%title%%'


# work out the bounding box so we can open the map at the correct center and zoom level

max_lat = None
min_lat = None
max_lon = None
min_lon = None

for i in df.index:
    lat = df.ix[i][lat_field]
    lon = df.ix[i][lon_field]
    if max_lat == None or lat > max_lat:
        max_lat = lat
    if min_lat == None or lat < min_lat:
        min_lat = lat
    if max_lon == None or lon > max_lon:
        max_lon = lon
    if min_lon == None or lon < min_lon:
        min_lon = lon

if tile_type == "StamenTerrain":
    tile_type = "Stamen Terrain"
if tile_type == "StamenToner":
    tile_type = "Stamen Toner"

if title:
    f = folium.element.Figure()
    f.html.add_child(folium.element.Element("<h2>"+title+"</h2>"))

m = folium.Map(tiles=tile_type,width=output_width,height=output_height)

if title:
    f.add_child(m)
else:
    f = m

m.fit_bounds([[min_lat,min_lon],[max_lat,max_lon]])

# add markers to the map

for i in df.index:
    if label_field:
        label = df.ix[i][label_field]
    else:
        label = ""
    lat = df.ix[i][lat_field]
    lon = df.ix[i][lon_field]

    args = {}
    if label_field:
        args["popup"] = str(label)

    if marker_type == "simple":
        folium.Marker([lat,lon],**args).add_to(m)
    else:
        if circle_radius_type == "radius_field":
            r = df.ix[i][circle_radius_field]
        else:
            r = circle_radius
        args["fill_color"] = circle_color
        args["radius"] = r
        args["color"] = circle_outline_color
        folium.CircleMarker([lat,lon],**args).add_to(m)
            
# output the map, obtain a path to write the HTML

if output_option == 'output_to_file':
    if not output_path:
        raise Exception("No output path specified")
else:
    output_path = os.tempnam()+".html"

f.save(output_path)

# launch the viewer if requested

if output_option == 'output_to_screen':
    os.system(viewer_command+" "+output_path)
    print("Output should open in a browser window")
else:
    print("Output should be saved on the server to path: "+output_path)

