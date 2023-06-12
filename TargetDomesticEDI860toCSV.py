from datetime import datetime
from pandas import DataFrame
import pyx12.x12context
import pyx12
from os import path, scandir, makedirs
import logging
from shutil import move
from configparser import ConfigParser
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from string import Template
from pathlib import Path
from email.mime.base import MIMEBase
from email.encoders import encode_base64

csv = []
tmp_list = []
code = ""
status = ""
exec_path = path.dirname(path.abspath(__file__))
config = ConfigParser()
config.read(exec_path + '\config.ini')

logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(config.get('LOG', 'PATH')+'Inbound_ToCSVTranslation_HK-'+datetime.today().strftime('%Y%m%d')+'.log')], format='%(asctime)s %(shortcode)s [%(levelname).3s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.shortcode = "TGTD"
    return record
logging.setLogRecordFactory(record_factory)

def isStrip (data):
    if not data is None:
        data = data.strip()
        return data
    else:
        return ""

def search (segment, parameter):
    for count in range(1, 16):
        if (seg.get_value(segment + str(count).zfill(2)) == parameter):
            return seg.get_value(segment + str(count + 1).zfill(2))
    return ""

OUTPUT_PATH = config.get('OUTPUT', 'PATH')
XFAILED_PATH = config.get('XFAILED', 'PATH')
REPORTED_PATH = config.get('REPORTED', 'PATH')

for input in config['INPUT']:
    folder = config['INPUT'][input]
    subfolder = path.basename(path.normpath(folder))
    for file in scandir(folder):
        ISA0506 = ISA0708 = ISA09 = ISA10 = ISA13 = GS06 = ""
        ST01 = ST02 = BCH01 = BCH02 = BCH03 = BCH06 = BCH08 = BCH11 = ""
        AR_REF02 = DP_REF02 = IA_REF02 = IA_REF03 = PD_REF02 = DP_REF03 = FOB01 = DE_FOB03 = OR_FOB03 = ZZ_FOB03 = CSH01 = SAC01 = SAC03 = SAC04 = TD501 = TD503_2 = TD503_92 = TD504 = TD505 = ""
        ITD01 = ITD02 = ITD03 = ITD04 = ITD05 = ITD06 = ITD07 = ITD12 = ITD13 = ""
        DTM010 = DTM015 = DTM037 = DTM038 = DTM063 = DTM064 = DTM078 = DTM118 = ""
        N902 = MSG01 = ""
        N101 = N102 = N104 = N201 = N301 = N302 = N301_2 = N302_2 = N401 = N402 = N403 = N404 = ""
        POC01 = POC02 = POC03 = POC04 = POC05 = POC06 = POC07 = CB_POC = EN_POC = EO_POC = IN_POC = UP_POC = VA_POC = RES_CTP03 = UCP_CTP03 = RES_CTP11 = UCP_CTP11 = PID05_08 = PID05_73 = PID05_74 = POC_PID04 = PO401 = PO414 = SAC01 = SAC04 = SAC04 = SAC13 = ""
        SDQ01 = SDQ03 = SDQ04 = ""
        SLN01 = SLN04 = SLN05 = SLN06 = SLN07 = CB_SLN = EN_SLN = EO_SLN = IN_SLN = UP_SLN = VA_SLN = SLN_PID04 = SLN_PID05 = ""
        CTT01 = CTT02 = ""
        first_list = second_list = third_list = forth_list = fifth_list = sixth_list = seventh_list = eighth_list = ninth_list = tenth_list = False
        POC = False
        forth_msg = []
        sixth_msg = []
        eighth_msg = []
        ninth_msg = []
        tenth_msg = []
        try:
            logging.info("Start translation: " + file.path)
            print("Start translation: ", file.path)
            src = pyx12.x12file.X12Reader(file)
            for seg in src:
                #first
                if (seg.get_seg_id() == "ISA"):
                    first_list = True
                    ISA0506 = isStrip(seg.get_value("ISA05")) + "/" + isStrip(seg.get_value("ISA06"))
                    ISA0708 = isStrip(seg.get_value("ISA07")) + "/" + isStrip(seg.get_value("ISA08"))
                    ISA09 = isStrip(seg.get_value("ISA09"))
                    ISA10 = isStrip(seg.get_value("ISA10"))
                    ISA13 = isStrip(seg.get_value("ISA13"))

                if (seg.get_seg_id() == "GS"):
                    first_list = True
                    GS06 = isStrip(seg.get_value("GS06"))

                #second
                if (seg.get_seg_id() == "ST"):
                    second_list = True
                    ST01 = isStrip(seg.get_value("ST01"))
                    ST02 = isStrip(seg.get_value("ST02"))
        
                if (seg.get_seg_id() == "BCH"):
                    second_list = True
                    BCH01 = isStrip(seg.get_value("BCH01"))
                    BCH02 = isStrip(seg.get_value("BCH02"))
                    BCH03 = isStrip(seg.get_value("BCH03"))
                    BCH06 = isStrip(seg.get_value("BCH06"))
                    BCH08 = isStrip(seg.get_value("BCH08"))
                    BCH11 = isStrip(seg.get_value("BCH11"))

                #third
                if (seg.get_seg_id() == "REF"):
                    third_list = True
                    if (isStrip(seg.get_value("REF01")) == "AR"):
                        AR_REF02 = isStrip(seg.get_value("REF02"))
                    if (isStrip(seg.get_value("REF01")) == "DP"):
                        DP_REF02 = isStrip(seg.get_value("REF02"))
                        DP_REF03 = isStrip(seg.get_value("REF03"))
                    if (isStrip(seg.get_value("REF01")) == "IA"):
                        IA_REF02 = isStrip(seg.get_value("REF02"))
                        IA_REF03 = isStrip(seg.get_value("REF03"))
                    if (isStrip(seg.get_value("REF01")) == "PD"):
                        PD_REF02 = isStrip(seg.get_value("REF02"))
        
                if (seg.get_seg_id() == "FOB"):
                    third_list = True
                    FOB01 = isStrip(seg.get_value("FOB01"))
                    if (isStrip(seg.get_value("FOB02")) == "DE"):
                        DE_FOB03 = isStrip(seg.get_value("FOB03"))
                    elif (isStrip(seg.get_value("FOB02")) == "OR"):
                        OR_FOB03 = isStrip(seg.get_value("FOB03"))
                    elif (isStrip(seg.get_value("FOB02")) == "ZZ"):
                        ZZ_FOB03 = isStrip(seg.get_value("FOB03"))

                if (seg.get_seg_id() == "CSH"):
                    third_list = True
                    CSH01 = isStrip(seg.get_value("CSH01"))

                if (seg.get_seg_id() == "SAC"):
                    third_list = True
                    SAC01 = isStrip(seg.get_value("SAC01"))
                    SAC03 = isStrip(seg.get_value("SAC03"))
                    SAC04 = isStrip(seg.get_value("SAC04"))
    
                if (seg.get_seg_id() == "TD5"):
                    third_list = True
                    TD501 = isStrip(seg.get_value("TD501"))
                    if (isStrip(seg.get_value("TD502")) == "2"):
                        TD503_2 = isStrip(seg.get_value("TD503"))
                    elif (isStrip(seg.get_value("TD502")) == "92"):
                        TD503_92 = isStrip(seg.get_value("TD503"))
                    TD504 = isStrip(seg.get_value("TD504"))
                    TD505 = isStrip(seg.get_value("TD505"))

                #forth
                if (seg.get_seg_id() == "ITD"):
                    if (forth_list):
                        tmp = (ITD01, ITD02, ITD03, ITD04, ITD05, ITD06, ITD12, ITD13)
                        forth_msg.append(tmp)
                        ITD01 = ITD02 = ITD03 = ITD04 = ITD05 = ITD06 = ITD12 = ITD13 = ""
                    forth_list = True
                    ITD01 = isStrip(seg.get_value("ITD01"))
                    ITD02 = isStrip(seg.get_value("ITD02"))
                    ITD03 = isStrip(seg.get_value("ITD03"))
                    ITD04 = isStrip(seg.get_value("ITD04"))
                    ITD05 = isStrip(seg.get_value("ITD05"))
                    ITD06 = isStrip(seg.get_value("ITD06"))
                    ITD07 = isStrip(seg.get_value("ITD07"))
                    ITD12 = isStrip(seg.get_value("ITD12"))
                    ITD13 = isStrip(seg.get_value("ITD13"))

                #fifth
                if (seg.get_seg_id() == "DTM"):
                    fifth_list = True
                    if (isStrip(seg.get_value("DTM01")) == "010"):
                        DTM010 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "015"):
                        DTM015 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "037"):
                        DTM037 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "038"):
                        DTM038 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "063"):
                        DTM063 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "064"):
                        DTM064 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "078"):
                        DTM078 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))
                    if (isStrip(seg.get_value("DTM01")) == "118"):
                        DTM118 = isStrip(seg.get_value("DTM02")) + isStrip(seg.get_value("DTM03")) + isStrip(seg.get_value("DTM04"))

                #sixth
                if (seg.get_seg_id() == "N9"):
                    sixth_list = True
                    N902 = isStrip(seg.get_value("N902"))

                if (seg.get_seg_id() == "MSG"):
                    if (MSG01 != ""):
                        tmp = (N902, MSG01)
                        sixth_msg.append(tmp)
                    sixth_list = True
                    MSG01 = isStrip(seg.get_value("MSG01"))

                #seventh
                if (seg.get_seg_id() == "N1"):
                    seventh_list = True
                    N101 = isStrip(seg.get_value("N101"))
                    N102 = isStrip(seg.get_value("N102"))
                    N104 = isStrip(seg.get_value("N104"))

                if (seg.get_seg_id() == "N2" and N101 == "MF"):
                    N201 = isStrip(seg.get_value("N201"))

                if (seg.get_seg_id() == "N3"):
                    if (N301 != "" and N302 != ""):
                        N301_2 = isStrip(seg.get_value("N301"))
                        N302_2 = isStrip(seg.get_value("N302"))
                    N301 = isStrip(seg.get_value("N301"))
                    N302 = isStrip(seg.get_value("N302"))
        
                if (seg.get_seg_id() == "N4"):
                    N401 = isStrip(seg.get_value("N401"))
                    N402 = isStrip(seg.get_value("N402"))
                    N403 = isStrip(seg.get_value("N403"))
                    N404 = isStrip(seg.get_value("N404"))

                #eighth
                if (seg.get_seg_id() == "POC"):
                    if (eighth_list):
                        tmp = (POC01, POC02, POC03, POC04, POC05, POC06, POC07, CB_POC, EN_POC, EO_POC, IN_POC, UP_POC, VA_POC, RES_CTP03, UCP_CTP03, RES_CTP11, UCP_CTP11, PID05_08, PID05_73, PID05_74, POC_PID04, PO401, PO414, SAC01, SAC04, SAC04, SAC13)
                        eighth_msg.append(tmp)
                        POC01 = POC02 = POC03 = POC04 = POC05 = POC06 = POC07 = CB_POC = EN_POC = EO_POC = IN_POC = UP_POC = VA_POC = RES_CTP03 = UCP_CTP03 = RES_CTP11 = UCP_CTP11 = PID05_08 = PID05_73 = PID05_74 = POC_PID04 = PO401 = PO414 = SAC01 = SAC04 = SAC04 = SAC13 = ""
                    eighth_list = True
                    POC = True
                    POC01 = isStrip(seg.get_value("POC01"))
                    POC02 = isStrip(seg.get_value("POC02"))
                    POC03 = isStrip(seg.get_value("POC03"))
                    POC04 = isStrip(seg.get_value("POC04"))
                    POC05 = isStrip(seg.get_value("POC05"))
                    POC06 = isStrip(seg.get_value("POC06"))
                    POC07 = isStrip(seg.get_value("POC07"))
                    CB_POC = search("POC", "CB")
                    EN_POC = search("POC", "EN")
                    EO_POC = search("POC", "EO")
                    IN_POC = search("POC", "IN")
                    UP_POC = search("POC", "UP")
                    VA_POC = search("POC", "VA")

                if (seg.get_seg_id() == "CTP"):
                    if (isStrip(seg.get_value("CTP02")) == "RES"):
                        RES_CT03 = isStrip(seg.get_value("CTP03"))
                        RES_CTP11 = isStrip(seg.get_value("CTP11"))
                    elif (isStrip(seg.get_value("CTP02")) == "UCP"):
                        UCP_CTP03 = isStrip(seg.get_value("CTP03"))
                        UCP_CTP11 = isStrip(seg.get_value("CTP11"))

                if (seg.get_seg_id() == "PID" and eighth_list):
                    if (isStrip(seg.get_value("PID02")) == "08"):
                        PID05_08 = isStrip(seg.get_value("PID05")) 
                    elif (isStrip(seg.get_value("PID02")) == "73"):
                        PID05_73 = isStrip(seg.get_value("PID05"))
                    elif (isStrip(seg.get_value("PID02")) == "74"):
                        PID05_74 = isStrip(seg.get_value("PID05"))
                    POC_PID04 = isStrip(seg.get_value("PID04"))

                if (seg.get_seg_id() == "PO4" and eighth_list):
                    PO401 = isStrip(seg.get_value("PO401"))
                    PO414 = isStrip(seg.get_value("PO414"))

                #ninth
                if (seg.get_seg_id() == "SDQ"):
                    if (ninth_list):
                        tmp = (SDQ01, SDQ03, SDQ04)
                        ninth_msg.append(tmp)
                        SDQ01 = SDQ03 = SDQ04 = ""
                    ninth_list = True
                    SDQ01 = isStrip(seg.get_value("SDQ01"))
                    SDQ03 = isStrip(seg.get_value("SDQ03"))
                    SDQ04 = isStrip(seg.get_value("SDQ04"))

                #tenth
                if (seg.get_seg_id() == "SLN"):
                    if (eighth_list):
                        eighth_list = False
                    if (tenth_list):
                        tmp = (SLN01, SLN04, SLN05, SLN06, SLN07, CB_SLN, EN_SLN, EO_SLN, IN_SLN, UP_SLN, VA_SLN, SLN_PID04, SLN_PID05)
                        tenth_msg.append(tmp)
                        SLN01 = SLN04 = SLN05 = SLN06 = SLN07 = CB_SLN = EN_SLN = EO_SLN = IN_SLN = UP_SLN = VA_SLN = SLN_PID04 = SLN_PID05 = ""
                    tenth_list = True
                    SLN01 = isStrip(seg.get_value("SLN01"))
                    SLN04 = isStrip(seg.get_value("SLN04"))
                    SLN05 = isStrip(seg.get_value("SLN05"))
                    SLN06 = isStrip(seg.get_value("SLN06"))
                    SLN07 = isStrip(seg.get_value("SLN07"))
                    CB_SLN = search("SLN", "CB")
                    EN_SLN = search("SLN", "EN")
                    EO_SLN = search("SLN", "EO")
                    IN_SLN = search("SLN", "IN")
                    UP_SLN = search("SLN", "UP")
                    VA_SLN = search("SLN", "VA")

                if (seg.get_seg_id() == "PID" and tenth_list):
                    tenth_list = True
                    SLN_PID04 = isStrip(seg.get_value("PID04"))
                    SLN_PID05 = isStrip(seg.get_value("PID05"))
        
                #eleventh
                if (seg.get_seg_id() == "CTT"):
                    CTT01 = isStrip(seg.get_value("CTT01"))
                    CTT02 = isStrip(seg.get_value("CTT02"))
                    if (tenth_list):
                        eighth_list = True
                    if (first_list):
                        csv.extend([("E", ISA0506, ISA0708, ISA09, ISA10, ISA13, GS06)])
                    if (second_list):
                        csv.extend([("P", ST01, ST02, BCH01, BCH02, BCH03, BCH06, BCH08, BCH11)])
                    if (third_list):
                        csv.extend([("I", AR_REF02, DP_REF02, IA_REF02, IA_REF03, PD_REF02, DP_REF03, FOB01, DE_FOB03, OR_FOB03, ZZ_FOB03, CSH01, SAC01, SAC03, SAC04, TD501, TD503_2, TD503_92, TD504, TD505)])
                    if (forth_list):
                        for msg in forth_msg:
                            msg = ("T",) + msg
                            csv.append(msg)
                        csv.extend([("T", ITD01, ITD02, ITD03, ITD04, ITD05, ITD06, ITD07, ITD12, ITD13)])
                    if (fifth_list):
                        csv.extend([("D", DTM010, DTM015, DTM037, DTM038, DTM063, DTM064, DTM078, DTM118)])
                    if (sixth_list):
                        for msg in sixth_msg:
                            msg = ("M",) + msg
                            csv.append(msg)
                        csv.extend([("M", N902, MSG01)])
                    if (seventh_list):
                        if (N101 == "BS"):
                            csv.extend([("A", N102, N104, N301, N302, N301_2, N302_2, N401, N402, N403, N404)])
                        elif (N101 == "BY"):
                            csv.extend([("A", "", "", "", "", "", "", "", "", "", "", N102, N104, N301, N302, N301_2, N302_2, N401, N402, N403, N404)])
                        elif (N101 == "MF"):
                            csv.extend([("A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", N102, N104, N201, N301, N302, N301_2, N302_2, N401, N402, N403, N404)])
                        elif (N101 == "ST"):
                            csv.extend([("A", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", N102, N104, N301, N302, N301_2, N302_2, N401, N402, N403, N404)])
                    if (eighth_list):
                        for msg in eighth_msg:
                            msg = ("IC",) + msg
                            csv.append(msg)
                        csv.extend([("IC", POC01, POC02, POC03, POC04, POC05, POC06, POC07, CB_POC, EN_POC, EO_POC, IN_POC, UP_POC, VA_POC, RES_CTP03, UCP_CTP03, RES_CTP11, UCP_CTP11, PID05_08, PID05_73, PID05_74, POC_PID04, PO401, PO414, SAC01, SAC04, SAC04, SAC13)])
                    if (ninth_list):
                        for msg in ninth_msg:
                            msg = ("S",) + msg
                            csv.append(msg)
                        csv.extend([("S", SDQ01, SDQ03, SDQ04)])
                    if (tenth_list):
                        for msg in tenth_msg:
                            msg = ("PP",) + msg
                            csv.append(msg)
                        csv.extend([("PP", SLN01, SLN04, SLN05, SLN06, SLN07, CB_SLN, EN_SLN, EO_SLN, IN_SLN, UP_SLN, VA_SLN, SLN_PID04, SLN_PID05)])
                    csv.extend([("CT", CTT01, CTT02)])
                    df = DataFrame(csv) 
                    logging.info("Translation ended!")
                    output = OUTPUT_PATH +subfolder+"/"+ path.splitext(file.name)[0] + ".csv"
                    df.to_csv(output, sep='\t', header=False, index=False)
                    csv = []

        except (pyx12.errors.XML_Reader_Error, pyx12.errors.X12Error, pyx12.errors.GSError, pyx12.errors.EngineError, pyx12.errors.IterOutOfBounds, pyx12.errors.IsValidError) as err:
            logging.info("Input file " + file.path + " ERROR occured: " + str(err))
            print(err, "here", BCH03)
            status = "xfailed"
            smtp = SMTP(host=config.get('EMAIL_LOGIN', 'HOST'), port=config.get('EMAIL_LOGIN', 'PORT'))
            smtp.ehlo()
            smtp.login(config.get('EMAIL_LOGIN', 'AC'), config.get('EMAIL_LOGIN', 'PW'))
            content = MIMEMultipart()
            content["from"] = config.get('EMAIL', 'SEND_FROM')
            content["to"] = config.get('EMAIL', 'SEND_TO_EDI')
            content["cc"] = config.get('EMAIL', 'SEND_TO_EDI_CC')
            content["subject"] = "TLT ALERT: Target Domestic EDI860 Warning (Translation Failure)"
            template = Template(Path(exec_path + "/Email_template/failure_cases.html").read_text(encoding="utf-8"))
            body = template.substitute({ "po_num": BCH03 , "source_file": file.name })
            content.attach(MIMEText(body, "html")) 
            attachement = MIMEBase('application', "octet-stream")
            with open(file, 'rb') as errorfile:
                attachement.set_payload(errorfile.read())
            encode_base64(attachement)
            attachement.add_header('Content-Disposition', 'attachment', filename=path.basename(file))
            content.attach(attachement)
            smtp.send_message(content)
            status = "reported"
            print("Error occured, email sent!")
        
        if (status == "xfailed"):
            move(file.path, XFAILED_PATH+"/"+file.name)
        elif (status == "reported"):
            move(file.path, REPORTED_PATH+"/"+file.name)
        else:
            del(src)
            print(file.path)
            move(file.path, config.get('ARCHIVE', 'PATH')+subfolder+"/"+file.name)
            logging.info("Successfully translated output file: "+ output)
            print("Translation done: ", output)    
        status = ""

    logging.info("There is no files found in " + folder)
    print("There is no files found in " + folder)
    