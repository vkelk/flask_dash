from pprint import pprint
import re


def striprtf(text):
    pattern = re.compile(r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)", re.I)
    # control words which specify a "destionation".
    destinations = frozenset((
        'aftncn','aftnsep','aftnsepc','annotation','atnauthor','atndate','atnicn','atnid',
        'atnparent','atnref','atntime','atrfend','atrfstart','author','background',
        'bkmkend','bkmkstart','blipuid','buptim','category','colorschememapping',
        'colortbl','comment','company','creatim','datafield','datastore','defchp','defpap',
        'do','doccomm','docvar','dptxbxtext','ebcend','ebcstart','factoidname','falt',
        'fchars','ffdeftext','ffentrymcr','ffexitmcr','ffformat','ffhelptext','ffl',
        'ffname','ffstattext','field','file','filetbl','fldinst','fldrslt','fldtype',
        'fname','fontemb','fontfile','fonttbl','footer','footerf','footerl','footerr',
        'footnote','formfield','ftncn','ftnsep','ftnsepc','g','generator','gridtbl',
        'header','headerf','headerl','headerr','hl','hlfr','hlinkbase','hlloc','hlsrc',
        'hsv','htmltag','info','keycode','keywords','latentstyles','lchars','levelnumbers',
        'leveltext','lfolevel','linkval','list','listlevel','listname','listoverride',
        'listoverridetable','listpicture','liststylename','listtable','listtext',
        'lsdlockedexcept','macc','maccPr','mailmerge','maln','malnScr','manager','margPr',
        'mbar','mbarPr','mbaseJc','mbegChr','mborderBox','mborderBoxPr','mbox','mboxPr',
        'mchr','mcount','mctrlPr','md','mdeg','mdegHide','mden','mdiff','mdPr','me',
        'mendChr','meqArr','meqArrPr','mf','mfName','mfPr','mfunc','mfuncPr','mgroupChr',
        'mgroupChrPr','mgrow','mhideBot','mhideLeft','mhideRight','mhideTop','mhtmltag',
        'mlim','mlimloc','mlimlow','mlimlowPr','mlimupp','mlimuppPr','mm','mmaddfieldname',
        'mmath','mmathPict','mmathPr','mmaxdist','mmc','mmcJc','mmconnectstr',
        'mmconnectstrdata','mmcPr','mmcs','mmdatasource','mmheadersource','mmmailsubject',
        'mmodso','mmodsofilter','mmodsofldmpdata','mmodsomappedname','mmodsoname',
        'mmodsorecipdata','mmodsosort','mmodsosrc','mmodsotable','mmodsoudl',
        'mmodsoudldata','mmodsouniquetag','mmPr','mmquery','mmr','mnary','mnaryPr',
        'mnoBreak','mnum','mobjDist','moMath','moMathPara','moMathParaPr','mopEmu',
        'mphant','mphantPr','mplcHide','mpos','mr','mrad','mradPr','mrPr','msepChr',
        'mshow','mshp','msPre','msPrePr','msSub','msSubPr','msSubSup','msSubSupPr','msSup',
        'msSupPr','mstrikeBLTR','mstrikeH','mstrikeTLBR','mstrikeV','msub','msubHide',
        'msup','msupHide','mtransp','mtype','mvertJc','mvfmf','mvfml','mvtof','mvtol',
        'mzeroAsc','mzeroDesc','mzeroWid','nesttableprops','nextfile','nonesttables',
        'objalias','objclass','objdata','object','objname','objsect','objtime','oldcprops',
        'oldpprops','oldsprops','oldtprops','oleclsid','operator','panose','password',
        'passwordhash','pgp','pgptbl','picprop','pict','pn','pnseclvl','pntext','pntxta',
        'pntxtb','printim','private','propname','protend','protstart','protusertbl','pxe',
        'result','revtbl','revtim','rsidtbl','rxe','shp','shpgrp','shpinst',
        'shppict','shprslt','shptxt','sn','sp','staticval','stylesheet','subject','sv',
        'svb','tc','template','themedata','title','txe','ud','upr','userprops',
        'wgrffmtfilter','windowcaption','writereservation','writereservhash','xe','xform',
        'xmlattrname','xmlattrvalue','xmlclose','xmlname','xmlnstbl',
        'xmlopen',
    ))
    # Translation of some special characters.
    specialchars = {
        'par': '\n',
        'sect': '\n\n',
        'page': '\n\n',
        'line': '\n',
        'tab': '\t',
        'emdash': '\u2014',
        'endash': '\u2013',
        'emspace': '\u2003',
        'enspace': '\u2002',
        'qmspace': '\u2005',
        'bullet': '\u2022',
        'lquote': '\u2018',
        'rquote': '\u2019',
        'ldblquote': '\201C',
        'rdblquote': '\u201D',
        'trowd': '\n',
        'pard': "\t"
    }
    stack = []
    ignorable = False       # Whether this group (and all inside it) are "ignorable".
    ucskip = 1              # Number of ASCII characters to skip after a unicode character.
    curskip = 0             # Number of ASCII characters left to skip
    out = []                # Output buffer.
    for match in pattern.finditer(text.decode()):
        word,arg,hex,char,brace,tchar = match.groups()
        if brace:
            curskip = 0
            if brace == '{':
                # Push state
                stack.append((ucskip,ignorable))
            elif brace == '}':
                # Pop state
                ucskip,ignorable = stack.pop()
        elif char: # \x (not a letter)
            curskip = 0
            if char == '~':
                if not ignorable:
                    out.append('\xA0')
            elif char in '{}\\':
                if not ignorable:
                    out.append(char)
            elif char == '*':
                ignorable = True
        elif word: # \foo
            curskip = 0
            if word in destinations:
                ignorable = True
            elif ignorable:
                pass
            elif word in specialchars:
                out.append(specialchars[word])
            elif word == 'uc':
                ucskip = int(arg)
            elif word == 'u':
                c = int(arg)
                if c < 0: c += 0x10000
                if c > 127: out.append(chr(c)) #NOQA
                else: out.append(chr(c))
                curskip = ucskip
        elif hex: # \'xx
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                c = int(hex,16)
                if c > 127: out.append(chr(c)) #NOQA
                else: out.append(chr(c))
        elif tchar:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                out.append(tchar)
    return ''.join(out)

def parser(data):
    articles = []
    start = re.search(r'\tHD\t', data).start()
    for m in re.finditer(r'Document [a-zA-Z0-9]{25}\t', data):
        end = m.end()
        a = data[start:end].strip()
        a = '\t' + a
        articles.append(a)
        start = end
 
    # In each article, find all used Intelligence Indexing field codes. Extract
    # content of each used field code, and write to a CSV file.
 
    # All field codes (order matters)
    fields = ['HD', 'CR', 'WC', 'PD', 'ET', 'SN', 'SC', 'ED', 'PG', 'LA', 'CY', 'LP',
              'TD', 'CT', 'RF', 'CO', 'IN', 'NS', 'RE', 'IPC', 'IPD', 'PUB', 'AN']
 
    for a in articles:
        used = [f for f in fields if re.search(r'\t' + f + r'\t', a)]
        unused = [[i, f] for i, f in enumerate(fields) if not re.search(r'\t' + f + r'\t', a)]
        fields_pos = []
        for f in used:
            f_m = re.search(r'\t' + f + r'\t', a)
            f_pos = [f, f_m.start(), f_m.end()]
            fields_pos.append(f_pos)
        obs = []
        n = len(used)
        for i in range(0, n):
            used_f = fields_pos[i][0]
            start = fields_pos[i][2]
            if i < n - 1:
                end = fields_pos[i + 1][1]
            else:
                end = len(a)
            content = a[start:end].strip()
            obs.append(content)
        for f in unused:
            obs.insert(f[0], '')
        # obs.insert(0, file.split('/')[-1].split('.')[0])  # insert Company ID, e.g., GVKEY
        print(obs)
        dictionary = dict(zip(fields, obs))
        pprint(dictionary)
        exit()
        # cur.execute('''INSERT INTO articles
        #                (id, hd, cr, wc, pd, et, sn, sc, ed, pg, la, cy, lp, td, ct, rf,
        #                co, ina, ns, re, ipc, ipd, pub, an)
        #                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        #                ?, ?, ?, ?, ?, ?, ?, ?)''', obs)

raw_data_folder = "rtfs/"
file1 = open(raw_data_folder + 'Factiva-20180619-1930.rtf', 'rb')
txt = file1.read()
# print(striprtf(txt))
with open('test.txt', 'w') as f: 
    f.write(striprtf(txt)) 
parser(striprtf(txt))
