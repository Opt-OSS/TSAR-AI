import logging
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from cartopy.io import img_tiles

log= logging.getLogger()
def get_cfeatures(resolution: int):
    if resolution not in [10, 50, 110]:
        raise ValueError('Resolution should be in [10,50,110]')
    res = f"{resolution}m"
    BORDERS = cfeature.NaturalEarthFeature('cultural', 'admin_0_boundary_lines_land',
                                           res, edgecolor='black', facecolor='none',
                                           linestyle=":",
                                           )
    """Small scale (1:110m) country boundaries."""

    STATES = cfeature.NaturalEarthFeature('cultural', 'admin_1_states_provinces_lakes',
                                          res, edgecolor='black', facecolor='none',
                                          linestyle=":"
                                          )
    """Small scale (1:110m) state and province boundaries."""

    COASTLINE = cfeature.NaturalEarthFeature('physical', 'coastline', res,
                                             edgecolor='black', facecolor='none')
    """Small scale (1:110m) coastline, including major islands."""

    LAKES = cfeature.NaturalEarthFeature('physical', 'lakes', res,
                                         edgecolor='face',
                                         facecolor=cfeature.COLORS['water'])
    """Small scale (1:110m) natural and artificial lakes."""

    LAND = cfeature.NaturalEarthFeature('physical', 'land', res,
                                        edgecolor='face',
                                        facecolor=cfeature.COLORS['land'], zorder=-1)
    """Small scale (1:110m) land polygons, including major islands."""

    OCEAN = cfeature.NaturalEarthFeature('physical', 'ocean', res,
                                         edgecolor='face',
                                         facecolor=cfeature.COLORS['water'], zorder=-1)
    """Small scale (1:110m) ocean polygons."""

    RIVERS = cfeature.NaturalEarthFeature('physical', 'rivers_lake_centerlines', res,
                                          edgecolor=cfeature.COLORS['water'],
                                          facecolor='none')

    POPULATED_PLACES = cfeature.NaturalEarthFeature('cultural', 'populated_places',
                                                    res, edgecolor='black', facecolor='black',
                                                    )

    return {
        'OCEAN': OCEAN,
        'LAND': LAND,
        'COASTLINE': COASTLINE,
        'BORDERS': BORDERS,
        'STATES': STATES,
        'LAKES': LAKES,
        'RIVERS': RIVERS,
        # 'POPULATED_PLACES': POPULATED_PLACES
    }

    # return [LAKES]

def add_features(ax, cf):
    # [ax.add_feature(x) for x in cf]
    for x in cf.values():
        # print(x)
        ax.add_feature(x)
import cartopy.crs as ccrs
def add_grid(ax, crs):
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.xlabels_top = False
    gl.ylabels_left = False
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER

def add_basemap(basemap, ax, crs, zoom=8):
    if basemap != 'naturalearth':
        # log.error(f"{basemap}")
        ax.add_image(img_tiles.Stamen(basemap), zoom)
    else:
        z = 110
        if zoom > 5:
            z = 50
        if zoom >= 9:
            z = 10
        # print(f"================{zoom} {z}")
        add_features(ax, get_cfeatures(z))
    add_grid(ax, crs)
