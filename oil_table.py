import requests
from bs4 import BeautifulSoup
import csv
from tkinter import *
from tkinter import ttk
import re
import os.path
import threading
import time


"""
23/11/2015
Generates a GUI which enables us to specify a numeric or range of numerics 
to gather data from the UK Well Search Tool 
(https://itportal.decc.gov.uk/pls/wons/wdep0100.qryWell). 

Ranges are specified by two numerics seperated by a hyphen i.e 1-30

There are currently 220 quadrants with at most 30 blocks each.

If we want to get all wells in a specified block, we can leave the well field empty. 
Block defaults to 1-30, which is the maximum block coverage. To specify all Quadrants, 
we can pass the quadrant field 1-220. 

The program will seek to not duplicate any entries. 
So the program can be run on the entire data table (Quadrant: 1-220, Block: 1-30) 
a second time in order to get any wells that may have been added between the first 
time we ran the program to mine the data and the second time.

The program writes to a csv file, oil_table.csv. 
Keep this file with this program as it will append to it as it reads new data. 


"""

def oil_grabber(search_url):
    """
    search_url is a single url of a page containing all the well pages links.
    Parses the HTML of the page with links to all the wells in the specified quadrant and block.

    """

    base_url = 'https://itportal.decc.gov.uk'

####Takes the search URL to bring up all the Well Registration numbers and links to point to the actual data
    response = requests.get(search_url)
    soup = BeautifulSoup(response.content)

###Finds all links in the HTML by the tag a and argument href.
###Removes the junk 'Go to top' and 'Go to bottom' links by list slicing the first. We only want links to the well data.
    raw_html = soup.find_all('a',href=True)
    raw_html_links = [html_link['href'] for html_link in raw_html]
    url_suffixes = raw_html_links[1:(len(raw_html_links)-1)]
    
    ##If Oil_data.csv does not already exist in the same dir, a new one is created with the correct header.
    if not (os.path.isfile('oil_data.csv')):
        header = ['Well Registration No.', 'Original Intent', 'Country Code', 'Onshore/Offshore', 'Quadrant No.', 'Block No.', 'Block Suffix', 'Platform', 'Drilling Sequence No.', 'Wellbore Type', 'Primary Target', 'Slot No.', 'Spud Date', 'Date TD Reached', 'Completion Date', 'Completion Status', 'Total MD Driller (feet)', 'Total MD Logger (feet)', 'TVDSS Driller', 'Datum Elevation (feet)', 'Datum Type', 'Water Depth (feet)', 'Ground Elevation (feet)', 'Deviated Well', 'Top hole Latitude', 'Top Hole Longtitude', 'Geodetic Datum', 'Coordinate System', 'Bottom Hole Latitude', 'Bottom Hole Longtitude']
        with open('oil_data.csv','w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)

    clean_urls = well_validator(url_suffixes)

    if len(clean_urls) > 0:
        for well_data_link in clean_urls:
            oil_link = base_url + well_data_link
            thread = threading.Thread(target=welldata_grabber,args=(oil_link,))
            thread.start()
            if threading.activeCount() > 100:
                print("sleeping...")
                time.sleep(2)
                
    success_popup()


def welldata_grabber(oil_link):
    """
    oil_link is the page where each well's data is kept
    Processes the data from each well page and appends it to an csv filtered
    """
    timings = []
    contents = ''
    
    well_page = requests.get(oil_link)
    oil_soup=BeautifulSoup(well_page.content)
    contents = oil_soup.text

###Strips the contents of all newline characters by splitting the string by it.
###This subsequent list is then filtered by removing all empty strings.
    contents = contents.split('\n')
    contents = list(filter(None,contents))

##Takes only the results we want, leaving behind all the categorical text
    raw_results = contents[13:72:2]
    
###Removes the leading ' = ' characters in each of our results strings
    clean_results = [leading_char.strip(' = ') for leading_char in raw_results]
    
    with threading.Lock():
        write_to_oil_data(clean_results)

def write_to_oil_data(clean_results):
##Opens oil_data.csv as a csv and appends to it with clean_results      
    with open('oil_data.csv','a') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(clean_results)

    
def block_grabber(quadrant,block):
    """
    quadrant,block are integers from the GUI given in the quad and block field
    This will generate links to all the wells in the quadrant/block specification.
    """

    quad_ext=''
    quad_url=''
    quadrant=quadrant.split('-')
    
    if len(quadrant) == 2:
        quadrant = [int(i) for i in quadrant]
        for i in range(quadrant[0],(quadrant[1] + 1)):
            quad_ext='&f_quadNoList='+ str(i)
            quad_url+=quad_ext
            
    else:
        quad_url='&f_quadNoList='+quadrant[0]
        
    block_ext=''
    block_url=''
    block=block.split('-')
    
    if len(block) == 2:
        block = [int(i) for i in block]
        for i in range(block[0],(block[1] + 1)):
            block_ext='&f_blockNoList='+ str(i)
            block_url+=block_ext
            
    elif len(block) == 1:
        block_url='&f_blockNoList='+block[0]

    search_url = 'https://itportal.decc.gov.uk/pls/wons/wdep0100.qryWell?f_quadNoList=***'+ quad_url + '&f_blockNoList=**' + block_url
    oil_grabber(search_url)

    success_popup()
    
def well_grabber(quadrant,block,wells):
    """
    quadrant,block,wells are integers given to the GUI in the string fields
    This is for getting a specific well's data.
    Creates a URL based on the ranges of values given as arguments from the GUI text areas. This url will point to a specific page containing that well's data. 

    Blocks can be given in the format of a number, or a number and letter. i.e 10 or 10a. re.findall('\d+|\D+',x) is used to split these up. 

    """
    LETTERS ='ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    block_code = re.findall('\d+|\D+', block)
    well_code = re.findall('\d+|\D+', wells)

    ##We add a '+' if the code only has a number attached to it, as opposed to a number,letter combo e.g 10a. Otherwise the suffix is 'a'. 
    if len(block_code) == 2:
        (block_no,block_suffix) = block_code
    else:
        block_code+=['+']
        (block_no,block_suffix) = block_code


    if well_code[0] in LETTERS:
        if len(well_code) == 3:
            (platform,drilling_seq,suffix) = well_code
        elif len(well_code) == 2:
            well_code+=['+']
            (platform,drilling_seq,suffix) = well_code
    else:
        if len(well_code) == 2:
            well_code.insert(0,'+')
            (platform,drilling_seq,suffix) = well_code

        else:
            ##If only a number is given as the wellcode, this will be the drilling sequence number. Platform and suffix default to '+' 
            well_code+=['+']
            well_code.insert(0,'+')
            (platform,drilling_seq,suffix) = well_code


    ##The finished well URL will pass the specified well's URL to welldata_grabber, for data processing.
    search_url='https://itportal.decc.gov.uk/pls/wons/wdep0100.wellHeaderData?p_quadNo=' + quadrant + '&p_blockNo=' + block_no + '&p_block_suffix=' + block_suffix + '&p_platform=' + platform + '&p_drilling_seq_no=' + drilling_seq + '&p_well_suffix=' + suffix

    valid_search = well_validator([search_url])
    if len(valid_search) > 0:
        welldata_grabber(search_url)

    success_popup()

def well_validator(clean_urls):
    """
    Checks whether a well is already in the csv file, and does not download the data if it is. Removes the already got well from the urls to be processed.
    """
    recorded_wells = []
    valid_urls = []

    with open('oil_data.csv','r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            recorded_wells += [(row['Well Registration No.'])]

    well_set = set(recorded_wells)

    while len(clean_urls) != 0:
        for url in clean_urls:
            url_info = []
            info = re.findall('=..?.?&|=.$',url)
            for data in info[0:(len(info)-1)]:
                url_info += [data[1:(len(data)-1)]]

            url_info += info[5][1]
            quadrant=url_info[0]

            if len(url_info[1]) == 1:
                full_block = '0' + url_info[1]
                if url_info[2] != '+':
                    full_block = ''.join((full_block,url_info[2]))

            else:
                if url_info[2] != '+':
                    full_block = ''.join((url_info[1],url_info[2]))
                else:
                    full_block = url_info[1]

            if url_info[3] != '+':
                well = ''.join((url_info[3],url_info[4]))

            else:
                well = ''.join((' ',url_info[4]))

            if url_info[5] != '+' and url_info[5] != 'R':
                well = ''.join((well,url_info[5]))

            code = ('{0}/{1}-{2}' .format(quadrant,full_block,well))

            if code in well_set:
                well_set.remove(code)
                clean_urls.remove(url)
            else:
                if len(clean_urls) == 0:
                    return(valid_urls)

                else:
                    valid_urls += [url]
                    clean_urls.remove(url)


    return(valid_urls)



def main():
    """
    Gets the arguments in each text box from the GUI and passes it to either well_grabber, if a specific well is given, or Block grabber, if a Block of quadrant is given. 
    """

    ##Grabbing the arguments given in the GUI
    q = quad.get()
    b = bloc.get()
    w = well.get()

    ##Determining whether the text fields are empty. 
    if len(q)!=0:
        if len(b) != 0:
            if len(w) != 0:
                well_grabber(q,b,w)

            else:
                block_grabber(q,b)
    else:
        parentgui = Tk()
        root.title("Oil Database query tool")
        maingui = ttk.Frame(parentgui,width=300,height=100)
        maingui.grid()
        quadrant_lbl = ttk.Label(maingui,text="Please enter a quadrant")
        quadrant_lbl.place(relx=0.5,rely=0.5,anchor=CENTER)
        
        parentgui.mainloop()


def success_popup():
    """
    Creates a popup window containing the words 'Successfully Downloaded!' when the processing of data is complete. 
    """
    parentgui = Tk()
    root.title("Oil Database query tool")
    maingui = ttk.Frame(parentgui,width=300,height=100)
    maingui.grid()
    quadrant_lbl = ttk.Label(maingui,text="Successfully Downloaded!")
    quadrant_lbl.place(relx=0.5,rely=0.5,anchor=CENTER)
    
    parentgui.mainloop()
        


"""
Building the GUI front end of the program. Consisting of one window, with three text areas (quad,bloc, well) to determine which data to get from the website. The quad field defaults to 1, the bloc field defaults to 1-30, and the well field has no default.
"""        
root = Tk()
window = ttk.Frame(root,padding="3 3 12 12")
window.grid(column=3, row=3)
root.title("Oil Database query tool")
        
quad = StringVar()
bloc = StringVar()
well = StringVar()

intro_label = ttk.Label(window,text="Which Quadrant would you like to download?")
quadrant_label = ttk.Label(window,text="Quadrant")
block_label = ttk.Label(window,text="Block No.")
well_label = ttk.Label(window,text="Well No.")

quadrant_entry = ttk.Entry(window,width=5,textvariable=quad)
block_entry = ttk.Entry(window,width=5,textvariable=bloc)
well_entry = ttk.Entry(window,width=5,textvariable=well)

quadrant_button = ttk.Button(window, text="Query")

intro_label.grid(columnspan=2)
quadrant_label.grid(column=0,row=2)
quadrant_entry.grid(column=1,row=2)
quad.set("1")
block_label.grid(column=0,row=3)
block_entry.grid(column=1,row=3)
bloc.set("1-30")
well_label.grid(column=0,row=4)
well_entry.grid(column=1,row=4)

quadrant_button.grid(columnspan=2)
quadrant_button.configure(command=main)

quadrant_entry.focus()
root.mainloop()
