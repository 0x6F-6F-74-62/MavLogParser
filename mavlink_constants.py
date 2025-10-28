
HEADER = b'\xA3\x95'
FMT_TYPE = 0x80
FMT_LENGTH = 89
FMT_MAPPING = {
    'a': '32h', 'b': 'b', 'B': 'B', 'h': 'h', 'H': 'H', 'i': 'i', 'I': 'I',
    'f': 'f', 'd': 'd', 'n': '4s', 'N': '16s', 'Z': '64s',
    'c': 'h', 'C': '100H', 'e': 'i', 'E': '100I', 'L': 'i', 'M': 'B', 'q': 'q', 'Q': 'Q'
}
SCALED = {'c', 'C', 'e', 'E'}      
LATLON = 'L'                       
ALT_MM = {'I', 'i'}    