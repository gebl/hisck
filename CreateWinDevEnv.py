import glob
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import libvirt
import libvirt_qemu
import sys, os, time
from zipfile import ZipFile
from tarfile import TarFile
import argparse
import subprocess
import sqlite3
import shutil
import hivex
import json
import base64
import requests
from tqdm import tqdm


timestr = time.strftime("%Y%m%d")

def handler(ctxt, err):
    global errno

    #print("handler(%s, %s)" % (ctxt, err))
    errno = err

libvirt.registerErrorHandler(handler, 'context')

def extractOVA(src, workpath):
    """
    1. Extract the file name from the path.
    2.  Check if the file is an OVA file.
    3.  Extract the OVA file to the working directory.
    4.  If the OVA file is not found, raise an exception.
    5.  Return the path to the extracted OVA file."""
    ovaname = None
    try:
        with ZipFile(src) as myzip:
            if myzip.namelist()[0].endswith(".ova"):
                ovaname = myzip.namelist()[0]
                path=os.path.join(workpath, ovaname)
                if not os.path.exists(path):
                    zi=myzip.getinfo(ovaname)
                    with open(path, 'wb') as of:
                        with tqdm(desc=path,total=zi.file_size,unit="B",unit_scale=True,unit_divisor=1024,) as bar:
                            with myzip.open(ovaname,'r') as inf:
                                while True:
                                    chunk = inf.read(1024)
                                    if not chunk:
                                        break
                                    bar.update(len(chunk)) 
                                    of.write(chunk)
    except:
        pass
    return ovaname


def extractVMDK(ova, workpath):
    """
    1. Opens the OVA file as a tar archive
    2. Iterates over the files in the archive
    3. If the file name ends with ".vmdk", remember that file
    4. If we didn't find any file ending with ".vmdk", raise an exception
    5. If the file isn't already on disk, extract it from the archive
    6. Return the name of the VMDK file"""
    print("ova: "+ova)
    print("workpath: "+workpath)
    vmdk = None
    with TarFile(os.path.join(workpath, ova)) as mytar:
        for n in mytar.getnames():
            if n.endswith(".vmdk"):
                vmdk = n
        if vmdk is None:
            raise Exception("Couldn't find OVA")
        path=os.path.join(workpath, vmdk)
        if not os.path.exists(path):
            ti=mytar.getmember(vmdk)
            with open(path, 'wb') as of:
                with tqdm(desc=path,total=ti.size,unit="B",unit_scale=True,unit_divisor=1024,) as bar:
                    with mytar.extractfile(vmdk) as inf:
                        while True:
                            chunk = inf.read(1024)
                            if not chunk:
                                break
                            bar.update(len(chunk)) 
                            of.write(chunk)
    return vmdk


def createBaseInstanceQCOW2(qcow2, iname):
    """
    1. We import the subprocess module, which lets us run commands in the terminal.
    2. We create a function called makeImg, which takes 2 arguments: iname and qcow2.
    3. The iname argument is used to name the new qcow2 image we will create.
    4. The qcow2 argument is the name of the base image we will use to create the new image.
    5. We use the subprocess module to run the qemu-img command, and pass the arguments listed above.
    6. We return the name of the new image to the caller."""
    print("Creating base image: " + iname + ".qcow2")
    process = subprocess.run(
        [
            "/usr/bin/qemu-img",
            "create",
            "-b",
            qcow2,
            "-F",
            "qcow2",
            "-f",
            "qcow2",
            iname + ".qcow2",
        ]
    )
    return iname + ".qcow2"


def translateQCOW2(vmdk, tmpdir):
    """
    1. Define a function called translateQCOW2 which takes a string called vmdk as input
    2. Create a string called qcow2 by replacing the last 5 characters of vmdk with .qcow2
    3. If there is no file with the name qcow2 in the current directory:
        1. Run the command "qemu-img convert -f vmdk -O qcow2 vmdk qcow2" in the terminal
    4. Return the string qcow2"""
    qcow2 = vmdk[:-5] + ".qcow2"
    totalsize=os.path.getsize(os.path.join(tmpdir, vmdk))*2.5
    if not os.path.exists(qcow2):
        with tqdm(desc=qcow2,total=totalsize,unit="B",unit_scale=True,unit_divisor=1024,) as bar:
            p=subprocess.Popen(
                [
                    "/usr/bin/qemu-img",
                    "convert",
                    "-f",
                    "vmdk",
                    "-O",
                    "qcow2",
                    os.path.join(tmpdir, vmdk),
                    qcow2,
                ],
            )
            last=0
            while p.poll() is None:
                try:
                    now=os.path.getsize(qcow2)
                except:
                    now=0
                bar.update(now-last)
                last=now
                time.sleep(1)
        bar.update((os.path.getsize(os.path.join(tmpdir, vmdk))*2.5)-os.path.getsize(qcow2))
    return qcow2


def createStorage(inputfile, instancename, tmpdir):
    ovaname = extractOVA(inputfile, tmpdir)
    vmdk = extractVMDK(ovaname, tmpdir)
    return translateQCOW2(vmdk, tmpdir)


def findInstanceName(instancename, conn):
    """The code above does the following, explained in English:
    1. We are going to create a new instance, and we need to check if the name of the instance is already in use.
    2. We start with the instance name given to us from the user.
    3. We then try to find an instance with that name. If we find it, then we append a dash and a number to the name and start over.
    4. We continue to look for the instance name, and increment the number on the end until we find a name that doesn't exist.
    5. When we find a name that doesn't exist, we return it."""
    i = 1
    iname = ""
    while True:
        try:
            if i == 0:
                iname = instancename
            else:
                iname = instancename + "-" + str(i)
            i += 1
            dom = conn.lookupByName(iname)
        except:
            dom = None
        if dom == None:
            break
    return iname


def defineXML(iname, qcow2list, iqcow2):
    """The code above does the following, explained in English:
    1. Open the XML file as a string.
    2. Use the beautiful soup module to parse the string into a document object.
    3. Get the domain of the document. (The top level tag in the XML is domain, so this is the root.)
    4. Find the name tag in the domain.
    5. Change the string in the name tag to the desired VM name.
    6. Find all the disk tags in the domain.
    7. For each disk tag, find the source tag, for the one that is a .qcow2 file:
        1. Change the file attribute of the source tag to the desired path to the image file.
        2. Add bcaking store nodes for each backing .qcow2 file.
    8. Convert the document object back to a string and return it."""
    with open("win11.xml", "r") as f:
        data = f.read()
    domainxml = BeautifulSoup(data, "xml")

    domainxml.domain.find("name").string = iname

    for disk in domainxml.domain.findAll("disk"):
        if disk.source["file"].endswith(".qcow2"):
            disk.source["file"] = os.path.join(os.getcwd(), iqcow2)
            tag = disk
            for bs in qcow2list:
                tag.backingStore["type"] = "file"
                tag.backingStore.append(domainxml.new_tag("format", type="qcow2"))
                tag.backingStore.append(
                    domainxml.new_tag("source", file=os.path.join(os.getcwd(), bs))
                )
                new_tag = domainxml.new_tag("backingStore")
                tag.backingStore.append(new_tag)
                tag = tag.backingStore
    return str(domainxml)


def bootVM(domainxml, conn):
    """The code above does the following, explained in English:
    1. Creates a connection to the virtualization software ( xen, qemu, etc. )
    2. Defines a domain object from the XML definition passed as a parameter
    3. Creates the domain
    4. Returns the domain object"""
    dom = conn.defineXML(str(domainxml))
    if not dom:
        raise SystemExit("Failed to define a domain from an XML definition")

    if dom.create() < 0:
        raise SystemExit("Can not boot guest domain")

    print("Guest " + dom.name() + " has booted")
    return dom


def runFdisk(img):
    """Run and return the output of the fdisk command."""
    cl = [
        "/usr/sbin/fdisk",
        "-l",
        img,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def connectNBD(dev, img):
    """Run and return the output of connecting the dev to /dev/nbd0"""
    cl = [
        "/usr/bin/qemu-nbd",
        "--connect=/dev/nbd0",
        img,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def disconnectNBD(dev):
    """Run and retrun the output of disconnecting the dev from /dev/nbd0"""
    cl = [
        "/usr/bin/qemu-nbd",
        "--disconnect",
        "/dev/nbd0",
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def mountWin(dev):
    """Mount dev on /mnt/win"""
    try:
        os.mkdir("/mnt/win")
    except:
        pass
    cl = [
        "/usr/bin/mount",
        dev,
        "/mnt/win",
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def umountWin(dev):
    """unmount dev"""
    cl = [
        "/usr/bin/umount",
        dev,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def getBackingFile(img):
    """Run qemu-img info to find the backing file for the img (only designed to with one backing file)"""
    cl = [
        "/usr/bin/qemu-img",
        "info",
        img,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    for l in process.stdout.decode("utf-8").split("\n"):
        if l.startswith("backing file:"):
            return l[14:]


def createSQLite(dev, dbpath):
    """Create a SQLLite database from the dev using tsk_loaddb"""
    cl = [
        "/usr/bin/tsk_loaddb",
        "-d",
        dbpath,
        "-k",
        "/dev/nbd0",
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def getParts(sconn):
    """ "Get parts from tsk_vs_parts"""
    # select  obj_id,addr,start,length,desc,flags from tsk_vs_parts
    cursor = sconn.cursor()
    rows = cursor.execute(
        "select  obj_id,addr,start,length,desc,flags from tsk_vs_parts order by start"
    )
    return rows.fetchall()


def getStartupPart(sconn):
    """Find the partition we need for mucking with startup"""
    cursor = sconn.cursor()
    rows = cursor.execute(
        "select distinct c.obj_id from tsk_files a, tsk_objects b, tsk_vs_parts c  where b.par_obj_id=c.obj_id and a.fs_obj_id=b.obj_id and a.parent_path like '/ProgramData/Microsoft/Windows/Start Menu/Programs/Startup/' and a.dir_type=3"
    )
    return rows.fetchall()[0][0]


def getPartNo(obj_id, parts):
    """Get the dev number for the partion"""
    index = 0
    for p in parts:
        if obj_id == p[0]:
            return index
        index += 1
    return -1


def getMountDev(sconn, dev):
    """Return the dev to mount for the windows partition"""
    parts = getParts(sconn)
    obj_id = getStartupPart(sconn)
    portno = getPartNo(obj_id, parts)
    return dev + "p" + str(portno)


def disableUAC():
    """Edit registry hive to disable UAC"""
    h = hivex.Hivex("/mnt/win/Windows/System32/config/SOFTWARE", write=True)
    key = h.root()
    key = h.node_get_child(key, "Microsoft")
    key = h.node_get_child(key, "Windows")
    key = h.node_get_child(key, "CurrentVersion")
    key = h.node_get_child(key, "Policies")
    key = h.node_get_child(key, "System")
    val = h.node_get_value(key, "EnableLUA")
    value1 = {"key": "EnableLUA", "t": 4, "value": b"\x00\x00\x00\x00"}
    h.node_set_value(key, value1)
    h.commit(None)


def setRunOnce():
    """Edit registry hive to disable UAC"""
    h = hivex.Hivex("/mnt/win/Windows/System32/config/SOFTWARE", write=True)
    key = h.root()
    key = h.node_get_child(key, "Microsoft")
    key = h.node_get_child(key, "Windows")
    key = h.node_get_child(key, "CurrentVersion")
    key = h.node_get_child(key, "RunOnce")
    value1 = {
        "key": "hisck",
        "t": 2,
        "value": "c:\\hisck\\startup.exe".encode("utf-16-le"),
    }
    h.node_set_value(key, value1)
    h.commit(None)


def copyFiles():
    """Copy startup.exe to the startup folder"""
    # shutil.copy(
    #    os.getcwd() + "/startup/startup.exe",
    #    "/mnt/win/ProgramData/Microsoft/Windows/Start Menu/Programs/Startup",
    # )
    os.makedirs("/mnt/win/hisck", exist_ok=True)
    shutil.copy(
        os.getcwd() + "/startup/startup.exe",
        "/mnt/win/hisck",
    )
    setRunOnce()


def qemuAgentCommand(
    domain, cmd, timeout=10, flag=libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT
):
    try:
        rawresult = libvirt_qemu.qemuAgentCommand(domain, cmd, timeout, flag)
        jsonresult = json.loads(rawresult)
        data = jsonresult
    except Exception as e:
        print(e.message)
        data = None
    return data


def copyFileGA(domain, fromPath, toPath):
    """Copy a file from the host to the guest"""
    status = {"execute": "guest-file-open", "arguments": {"path": toPath, "mode": "w"}}
    result = qemuAgentCommand(domain, json.dumps(status))
    if result is None:
        print("Error opening file")
        return
    handle = result["return"]

    totalsize=os.path.getsize(fromPath)
    with tqdm(desc="copy",total=totalsize,unit="B",unit_scale=True,unit_divisor=1024,) as bar:
        with open(fromPath, "rb") as f:
            while True:
                data = f.read(1024 * 32)
                bar.update(len(data))
                if not data:
                    break
                status = {
                    "execute": "guest-file-write",
                    "arguments": {
                        "handle": handle,
                        "buf-b64": base64.b64encode(data).decode("utf-8"),
                    },
                }
                result = qemuAgentCommand(domain, json.dumps(status))
        status = {"execute": "guest-file-close", "arguments": {"handle": handle}}
        result = qemuAgentCommand(domain, json.dumps(status))


def getStatus(domain, pid):
    """Get the status of the command running in the guest"""
    status = {"execute": "guest-exec-status", "arguments": {"pid": pid}}
    result = qemuAgentCommand(domain, json.dumps(status))
    return result


def runCmd(domain, cmd, args):
    """Run a command in the guest"""
    if len(args) == 0:
        args = []
    payload = {
        "execute": "guest-exec",
        "arguments": {"path": str(cmd), "arg": args, "capture-output": True},
    }

    result = qemuAgentCommand(domain, json.dumps(payload))
    try:
        pid = result["return"]["pid"]
    except:
        return None
    cmddone = False
    while not cmddone:
        out = getStatus(domain, pid)
        cmddone = out["return"]["exited"]
    return out


def parseStdoutStderr(raw):
    """Parse the stdout and stderr from the command run in the guest"""
    try:
        base64Out = base64.b64decode(raw["return"]["out-data"]).decode("UTF-8")
    except KeyError as e:
        base64Out = ""
    try:
        base64Err = base64.b64decode(raw["return"]["err-data"]).decode("UTF-8")
    except KeyError as e:
        base64Err = ""
    return base64Out, base64Err


def createCustomizedImage(f,tag,tmpdir,d, conn):
    """Create a image for customization from the inputfile"""
    dbname = f + ".db"
    if not os.path.exists(dbname):
        print("Creating SQL DB")
        connectNBD(d, f).decode("utf-8")
        print(runFdisk(d).decode("utf-8"))
        createSQLite(d, dbname).decode("utf-8")
        disconnectNBD(d).decode("utf-8")

    iname = tag
    iqcow2 = createBaseInstanceQCOW2(f, iname)

    sconn = sqlite3.connect(dbname)

    connectNBD(d, iqcow2).decode("utf-8")
    mountdev = getMountDev(sconn, d)
    mountWin(mountdev)

    print("Disabling UAC")
    disableUAC()
    print("Copying files")
    copyFiles()

    umountWin(mountdev)
    disconnectNBD(d).decode("utf-8")

    dxl = defineXML(iname, [f], iqcow2)
    print("Booting VM")
    dom = bootVM(dxl, conn)

    result = None
    sys.stdout.write("Connecting to Guest")
    while result is None:
        result = runCmd(dom, "cmd", ["/c", "whoami"])
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(5)
    print(result)
    print(base64.b64decode(result["return"]["out-data"]))

    print("Calling powershell to intall chocolatey")
    result = runCmd(
        dom,
        "powershell.exe",
        [
            "-NoProfile",
            "-InputFormat",
            "None",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "[System.Net.ServicePointManager]::SecurityProtocol = 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))",
        ],
    )
    print(result)
    print(base64.b64decode(result["return"]["out-data"]))

    dom.reboot()
    result = None
    sys.stdout.write("Connecting to Guest")
    while result is None:
        result = runCmd(dom, "cmd", ["/c", "whoami"])
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(15)
    print(base64.b64decode(result["return"]["out-data"]))

    with open("requirements.txt") as file:
        for package in file.readlines():
            try:
                print(package)
                result = runCmd(
                    dom,
                    "powershell.exe",
                    ["choco", "install", package.rstrip(), "-y"],
                )
                print(result)
                print(base64.b64decode(result["return"]["out-data"]))
            except:
                pass

    dom.shutdown()
    print("Shutting down...")
    while dom.isActive():
        time.sleep(5)

    dbname = iqcow2 + ".db"
    print(connectNBD(d, iqcow2).decode("utf-8"))
    print(runFdisk(d).decode("utf-8"))
    print(createSQLite(d, dbname).decode("utf-8"))
    print(disconnectNBD(d).decode("utf-8"))
    return iqcow2


def launchSubInstance(name, conn):
    """Launch an instance built from the customized image"""
    iname = findInstanceName(name, conn)
    print(iname)
    bf1 = name + ".qcow2"
    iqcow2 = createBaseInstanceQCOW2(bf1, iname)
    print(iqcow2)
    bf2 = getBackingFile(bf1)
    dxl = defineXML(iname, [bf1, bf2], iqcow2)
    dom = bootVM(dxl, conn)


def downloadUrl(url,dest):
    size=int(requests.head(url).headers['Content-Length'])
    
    read=0
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest, 'wb') as f:
            with tqdm(desc=dest,total=size,unit="B",unit_scale=True,unit_divisor=1024,) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    bar.update(len(chunk)) 
                    f.write(chunk)

    #cl = ["/usr/bin/wget", "-N", winurl, "-P", "downloads"]
    #process = subprocess.run(cl, capture_output=True, check=True)
    #return process.stdout

def downloadWinVm(winurl):
    req_headers = requests.head(winurl)
    winzip = req_headers.headers["Location"]
    winzipu = urlparse(winzip)
    dest = os.path.join("downloads", os.path.basename(winzipu.path))
    print("Downloading: "+dest +" from "+req_headers.headers["Location"]+"...")
    if not os.path.exists(dest):
        downloadUrl(req_headers.headers["Location"],dest)
        print("Download complete.")
    else:
        print("File already downloaded.")

def downloadVirtio(virtiourl):
    req_headers = requests.head(virtiourl)
    virtiozip = req_headers.headers["Location"]
    req = requests.get(virtiourl)
    soup = BeautifulSoup(req.content,features="lxml")
    for a in soup.findAll("a"):
        if a["href"].endswith(".iso") and a["href"].startswith("virtio-win-"):
            dest = os.path.join("downloads", a["href"])
            print("Downloading: "+dest+" from "+virtiozip + a["href"]+"...")
            if not os.path.exists(dest):
                downloadUrl(virtiozip + a["href"],dest)
            else:
                print("File already downloaded.")

def CreateWinTemplateVM(tag,winevalzip,tmpdir,d,conn):
    inputfile = os.path.join("downloads", os.path.basename(winevalzip))
    f=createStorage(inputfile, tag, "workdir")
    createCustomizedImage(f,tag,tmpdir,d, conn)
                
def main():
    commands=['downloadwineval','downloadvirtio','createwintemplate','createwininstance','copyfile','runps1']
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=commands)
    parser.add_argument(
        "--file", type=str, help="zipfile for Dev VM", default=None
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Name to use for defining VM.",
        default="win11vm",
    )
    parser.add_argument(
        "--virtioiso",
        type=str,
        help="url for windows download",
        default=None,
    )
    
    parser.add_argument(
        "--winevalzip",
        type=str,
        help="url for windows download",
        default=None,
    )
    
    
    parser.add_argument(
        "--tmpdir", type=str, help="location for workfiles", default="workdir"
    )
    parser.add_argument(
        "--virtiourl",
        type=str,
        help="location for workfiles",
        default="https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/latest-virtio/",
    )
    parser.add_argument(
        "--winevalurl",
        type=str,
        help="url for windows download",
        default="https://aka.ms/windev_VM_virtualbox",
    )
    
    parser.add_argument(
        "--dev", type=str, help="location for workfiles", default="/dev/nbd0"
    )
    parser.add_argument(
        "--fromPath", type=str, help="location for workfiles", default=None
    )
    parser.add_argument(
        "--toPath", type=str, help="location for workfiles", default="c:\\hisck\\"
    )
    parser.add_argument(
        "--cmd", type=str, help="location for workfiles", default="whoami"
    )
    args = parser.parse_args()
    # print(args)

    conn = libvirt.open("qemu:///system")

    list_of_files = glob.glob(
        "downloads/virtio-win*"
    )  # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    print(latest_file)

    match args.command:
        case 'downloadwineval':
            downloadWinVm(args.winevalurl)
        case 'downloadvirtio':
            downloadVirtio(args.virtiourl)
        case 'createwintemplate':
            CreateWinTemplateVM(args.tag,args.winevalzip,args.tmpdir,args.dev,conn)
        case 'createwininstance':
            try:
                conn.lookupByName(args.tag)
            except:
                raise Exception("Template VM not found")
            launchSubInstance(args.tag, conn)
        case 'copyfile':
            copyFileGA(conn.lookupByName(args.tag), args.fromPath, args.toPath)
        case 'runps1':
            result = runCmd(
                conn.lookupByName(args.tag),
                "powershell.exe",
                [
                    "-NoProfile",
                    "-InputFormat",
                    "None",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    args.cmd,
                ],
            )
            print(result)
            print(base64.b64decode(result["return"]["out-data"]).decode("utf-8"))

    conn.close()


if __name__ == "__main__":
    main()
