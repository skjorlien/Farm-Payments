from matplotlib.ticker import FuncFormatter


## Settings/Helpers
bilFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e9) + ' B')
milFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e6) + ' M')
thouFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e3) + ' T')

def get_float_formatter(seriesmean):
    fmtr = None
    if seriesmean > 1e3:
        fmtr = thouFormatter
    if seriesmean > 1e6: 
        fmtr = milFormatter
    if seriesmean > 1e9:
        fmtr = bilFormatter
    return fmtr

def update_settings(default: dict, newsettings: dict):
    if type(default) is not dict or type(newsettings) is not dict: 
        raise ValueError('Types of dictionaries not conformable')
    
    for key, val in newsettings.items():
        if type(val) is dict and key in default: 
            default[key] = update_settings(default[key], newsettings[key])
        else:
            default[key] = val
    return default
