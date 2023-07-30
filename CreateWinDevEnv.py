from bs4 import BeautifulSoup
import libvirt
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

timestr = time.strftime("%Y%m%d")


def extractOVA(src, workpath):
    """
    1. Extract the file name from the path.
    2.  Check if the file is an OVA file.
    3.  Extract the OVA file to the working directory.
    4.  If the OVA file is not found, raise an exception.
    5.  Return the path to the extracted OVA file. """
    ovaname = None
    with ZipFile(src) as myzip:
        if myzip.namelist()[0].endswith(".ova"):
            ovaname = myzip.namelist()[0]
            if not os.path.exists(ovaname):
                myzip.extract(ovaname)
    if ovaname is None:
        raise Exception("Couldn't find OVA")
    return ovaname


def extractVMDK(ova, workpath):
    """ 
    1. Opens the OVA file as a tar archive
    2. Iterates over the files in the archive
    3. If the file name ends with ".vmdk", remember that file
    4. If we didn't find any file ending with ".vmdk", raise an exception
    5. If the file isn't already on disk, extract it from the archive
    6. Return the name of the VMDK file """
    vmdk = None
    with TarFile(ova) as mytar:
        for n in mytar.getnames():
            if n.endswith(".vmdk"):
                vmdk = n
        if vmdk is None:
            raise Exception("Couldn't find OVA")
        if not os.path.exists(vmdk):
            mytar.extract(vmdk)
    return vmdk


def createBaseInstanceQCOW2(qcow2, iname):
    """ 
    1. We import the subprocess module, which lets us run commands in the terminal.
    2. We create a function called makeImg, which takes 2 arguments: iname and qcow2.
    3. The iname argument is used to name the new qcow2 image we will create.
    4. The qcow2 argument is the name of the base image we will use to create the new image.
    5. We use the subprocess module to run the qemu-img command, and pass the arguments listed above.
    6. We return the name of the new image to the caller. """
    process = subprocess.run(
        [
            "qemu-img",
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
    print(process.stdout)
    print(process.stderr)
    return iname + ".qcow2"


def translateQCOW2(vmdk):
    """ 
    1. Define a function called translateQCOW2 which takes a string called vmdk as input
    2. Create a string called qcow2 by replacing the last 5 characters of vmdk with .qcow2
    3. If there is no file with the name qcow2 in the current directory:
        1. Run the command "qemu-img convert -f vmdk -O qcow2 vmdk qcow2" in the terminal
    4. Return the string qcow2 """
    qcow2 = vmdk[:-5] + ".qcow2"
    if not os.path.exists(qcow2):
        subprocess.run(
            ["qemu-img", "convert", "-f", "vmdk", "-O", "qcow2", vmdk, qcow2]
        )
    return qcow2


def createStorage(inputfile, instancename, tmpdir):
    ovaname = extractOVA(inputfile, tmpdir)
    vmdk = extractVMDK(ovaname, tmpdir)
    return translateQCOW2(vmdk)


def findInstanceName(instancename, conn):
    """ The code above does the following, explained in English:
    1. We are going to create a new instance, and we need to check if the name of the instance is already in use.
    2. We start with the instance name given to us from the user.
    3. We then try to find an instance with that name. If we find it, then we append a dash and a number to the name and start over.
    4. We continue to look for the instance name, and increment the number on the end until we find a name that doesn't exist.
    5. When we find a name that doesn't exist, we return it. """
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
    """ The code above does the following, explained in English:
    1. Open the XML file as a string.
    2. Use the beautiful soup module to parse the string into a document object.
    3. Get the domain of the document. (The top level tag in the XML is domain, so this is the root.)
    4. Find the name tag in the domain.
    5. Change the string in the name tag to the desired VM name.
    6. Find all the disk tags in the domain.
    7. For each disk tag, find the source tag, for the one that is a .qcow2 file:
        1. Change the file attribute of the source tag to the desired path to the image file.
        2. Add bcaking store nodes for each backing .qcow2 file. 
    8. Convert the document object back to a string and return it. """
    with open("win11.xml", "r") as f:
        data = f.read()
    domainxml = BeautifulSoup(data, "xml")

    print(domainxml.domain.find("name"))
    domainxml.domain.find("name").string = iname
    print(domainxml.domain.find("name"))

    for disk in domainxml.domain.findAll("disk"):
        print(disk.driver["type"])
        if disk.source["file"].endswith(".qcow2"):
            disk.source["file"] = os.path.join(os.getcwd(), iqcow2)
            tag = disk
            for bs in qcow2list:
                print("------")
                print(tag)
                tag.backingStore["type"] = "file"
                tag.backingStore.append(domainxml.new_tag("format", type="qcow2"))
                tag.backingStore.append(
                    domainxml.new_tag("source", file=os.path.join(os.getcwd(), bs))
                )
                new_tag = domainxml.new_tag("backingStore")
                tag.backingStore.append(new_tag)
                tag = tag.backingStore
            print("#####")
            print(disk)

    for disk in domainxml.domain.findAll("disk"):
        print(disk.driver["type"])
        print(disk.source["file"])

    return str(domainxml)


def bootVM(domainxml, conn):
    """ The code above does the following, explained in English:
    1. Creates a connection to the virtualization software ( xen, qemu, etc. )
    2. Defines a domain object from the XML definition passed as a parameter
    3. Creates the domain
    4. Returns the domain object """
    dom = conn.defineXML(str(domainxml))
    if not dom:
        raise SystemExit("Failed to define a domain from an XML definition")

    if dom.create() < 0:
        raise SystemExit("Can not boot guest domain")

    print("Guest " + dom.name() + " has booted")
    return dom


def runFdisk(img):
    cl = [
        "/usr/sbin/fdisk",
        "-l",
        img,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def connectNBD(dev, img):
    cl = [
        "/usr/bin/qemu-nbd",
        "--connect=/dev/nbd0",
        img,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def disconnectNBD(dev):
    cl = [
        "/usr/bin/qemu-nbd",
        "--disconnect",
        "/dev/nbd0",
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def mountWin(dev):
    try:
        os.mkdir("/mnt/win")
    except:
        pass
    cl = [
        "mount",
        dev,
        "/mnt/win",
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def umountWin(dev):
    cl = [
        "umount",
        dev,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    return process.stdout


def getBackingFile(img):
    cl = [
        "qemu-img",
        "info",
        img,
    ]
    process = subprocess.run(cl, capture_output=True, check=True)
    print(process.stdout)
    for l in process.stdout.decode("utf-8").split("\n"):
        if l.startswith("backing file:"):
            return l[14:]


def createSQLite(dev, dbpath):
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
    # select  obj_id,addr,start,length,desc,flags from tsk_vs_parts
    cursor = sconn.cursor()
    rows = cursor.execute(
        "select  obj_id,addr,start,length,desc,flags from tsk_vs_parts order by start"
    )
    return rows.fetchall()


def getStartupPart(sconn):
    cursor = sconn.cursor()
    rows = cursor.execute(
        "select distinct c.obj_id from tsk_files a, tsk_objects b, tsk_vs_parts c  where b.par_obj_id=c.obj_id and a.fs_obj_id=b.obj_id and a.parent_path like '/ProgramData/Microsoft/Windows/Start Menu/Programs/Startup/' and a.dir_type=3"
    )
    return rows.fetchall()[0][0]


def getPartNo(obj_id, parts):
    index = 0
    for p in parts:
        if obj_id == p[0]:
            return index
        index += 1
    return -1


def getMountDev(sconn, dev):
    parts = getParts(sconn)
    obj_id = getStartupPart(sconn)
    portno = getPartNo(obj_id, parts)
    return dev + "p" + str(portno)


def disableUAC():
    h = hivex.Hivex("/mnt/win/Windows/System32/config/SOFTWARE", write=True)
    key = h.root()
    key = h.node_get_child(key, "Microsoft")
    key = h.node_get_child(key, "Windows")
    key = h.node_get_child(key, "CurrentVersion")
    key = h.node_get_child(key, "Policies")
    key = h.node_get_child(key, "System")
    val = h.node_get_value(key, "EnableLUA")
    print(h.value_value(val))
    value1 = {"key": "EnableLUA", "t": 4, "value": b"\x00\x00\x00\x00"}
    h.node_set_value(key, value1)
    h.commit(None)


def copyFiles():
    shutil.copy(
        os.getcwd() + "/startup/startup.exe",
        "/mnt/win/ProgramData/Microsoft/Windows/Start Menu/Programs/Startup",
    )


def getStatus(domain, pid):
    status = {"execute": "guest-exec-status", "arguments": {"pid": pid}}
    result = subprocess.run(
        [
            "virsh",
            "-c",
            "qemu:///system",
            "qemu-agent-command",
            domain,
            json.dumps(status),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def runCmd(domain, cmd, args):
    if len(args) == 0:
        args = []
    payload = {
        "execute": "guest-exec",
        "arguments": {"path": str(cmd), "arg": args, "capture-output": True},
    }

    p = subprocess.run(
        [
            "virsh",
            "-c",
            "qemu:///system",
            "qemu-agent-command",
            domain,
            json.dumps(payload),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if len((p.stderr.rstrip())) > 0:
        return None
    pid = json.loads(p.stdout)["return"]["pid"]
    cmddone = False
    while not cmddone:
        out = getStatus(domain, pid)
        cmddone = out["return"]["exited"]
    return out


def parseStdoutStderr(raw):
    try:
        base64Out = base64.b64decode(raw["return"]["out-data"]).decode("UTF-8")
    except KeyError as e:
        base64Out = ""
    try:
        base64Err = base64.b64decode(raw["return"]["err-data"]).decode("UTF-8")
    except KeyError as e:
        base64Err = ""
    return base64Out, base64Err


def createCustomizedImage(args, conn):
    qcow2 = createStorage(args.inputfile, args.instancename, args.tmpdir)

    dbname = qcow2 + ".db"
    if not os.path.exists(dbname):
        print(connectNBD(args.dev, qcow2).decode("utf-8"))
        print(runFdisk(args.dev).decode("utf-8"))
        print(createSQLite(args.dev, dbname).decode("utf-8"))
        print(disconnectNBD(args.dev).decode("utf-8"))

    iname = args.instancename
    iqcow2 = createBaseInstanceQCOW2(qcow2, iname)

    sconn = sqlite3.connect(dbname)

    print(connectNBD(args.dev, iqcow2).decode("utf-8"))
    mountdev = getMountDev(sconn, args.dev)
    print(mountWin(mountdev))

    disableUAC()
    copyFiles()

    print(umountWin(mountdev))
    print(disconnectNBD(args.dev).decode("utf-8"))

    dxl = defineXML(iname, [qcow2], iqcow2)
    dom = bootVM(dxl, conn)

    result = None
    sys.stdout.write("Connecting to Guest")
    while result is None:
        result = runCmd(iname, "cmd", ["/c", "whoami"])
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(5)
    print(result)
    stdout, stderr = parseStdoutStderr(result)
    print(stdout)

    result = runCmd(
        iname,
        "powershell.exe",
        [
            "-NoProfile",
            "-InputFormat",
            "None",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            'del "c:\ProgramData\Microsoft\Windows\Start Menu\Programs\Startup\startup.exe"',
        ],
    )
    print(result)
    stdout, stderr = parseStdoutStderr(result)
    print(stdout)

    result = runCmd(
        iname,
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
    stdout, stderr = parseStdoutStderr(result)
    print(stdout)

    dom.reboot()
    result = None
    sys.stdout.write("Connecting to Guest")
    while result is None:
        result = runCmd(iname, "cmd", ["/c", "whoami"])
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(15)
    print(result)
    stdout, stderr = parseStdoutStderr(result)
    print(stdout)

    with open("requirements.txt") as file:
        for package in file.readlines():
            try:
                print(package)
                result = runCmd(
                    iname,
                    "powershell.exe",
                    ["choco", "install", package.rstrip(), "-y"],
                )
                print(result)
                stdout, stderr = parseStdoutStderr(result)
                print(stdout)
            except:
                pass

    dom.shutdown()
    print("Shutting down...")
    while dom.isActive():
        time.sleep(5)

    dbname = iqcow2 + ".db"
    print(connectNBD(args.dev, iqcow2).decode("utf-8"))
    print(runFdisk(args.dev).decode("utf-8"))
    print(createSQLite(args.dev, dbname).decode("utf-8"))
    print(disconnectNBD(args.dev).decode("utf-8"))
    return iqcow2


def launchSubInstance(name, conn):
    iname = findInstanceName(name, conn)
    print(iname)
    bf1 = name + ".qcow2"
    iqcow2 = createBaseInstanceQCOW2(bf1, iname)
    print(iqcow2)
    bf2 = getBackingFile(bf1)
    dxl = defineXML(iname, [bf1, bf2], iqcow2)
    dom = bootVM(dxl, conn)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inputfile",
        type=str,
        help="zipfile for Dev VM",
        default="/home/gabe/Downloads/WinDev2306Eval.VirtualBox.zip",
    )
    parser.add_argument(
        "--instancename",
        type=str,
        help="Name to use for defining VM.",
        default="win11vm",
    )
    parser.add_argument("--tmpdir", type=str, help="location for workfiles", default="")
    parser.add_argument(
        "--gaurl",
        type=str,
        help="location for workfiles",
        default="https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/latest-qemu-ga/",
    )
    parser.add_argument(
        "--virtiourl",
        type=str,
        help="location for workfiles",
        default="https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/latest-virtio/",
    )
    parser.add_argument(
        "--dev", type=str, help="location for workfiles", default="/dev/nbd0"
    )
    args = parser.parse_args()
    print(args)
    conn = libvirt.open("qemu:///system")
    try:
        conn.lookupByName(args.instancename)
    except:
        createCustomizedImage(args, conn)

    launchSubInstance(args.instancename, conn)

    conn.close()


if __name__ == "__main__":
    main()
