package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func checkFile(fname, pattern string) bool {
	raw, err := os.ReadFile(fname)
	if err != nil {
		return false
	}

	// Make an tranformer that converts MS-Win default to UTF8:
	win16be := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM)
	// Make a transformer that is like win16be, but abides by BOM:
	utf16bom := unicode.BOMOverride(win16be.NewDecoder())

	// Make a Reader that uses utf16bom:
	unicodeReader := transform.NewReader(bytes.NewReader(raw), utf16bom)

	// decode and print:
	decoded, err := ioutil.ReadAll(unicodeReader)

	return strings.Contains(string(decoded), pattern)
}

func main() {
	fmt.Println("Install Drivers...")
	installDrivers()
	for {
		if checkFile("c:\\drv-log.txt", "=== Logging stopped:") {
			fmt.Println("Completed.")
			os.Remove("c:\\drv-log.txt")
			break
		}
	}
	fmt.Println("Install Guest Agent...")
	installGuestAgent()
	for {
		if checkFile("c:\\ga-log.txt", "=== Logging stopped:") {
			fmt.Println("Completed.")
			os.Remove("c:\\ga-log.txt")
			break
		}
	}
	fmt.Println("Install Spice Agent...")
	installSpiceAgent()
	fmt.Println("Launched.")
}

func installDrivers() {
	verb := "runas"
	exe := "msiexec"
	cwd := "D:\\"
	args := "/i \"virtio-win-gt-x64.msi\" /qn ADDLOCAL=ALL /l c:\\drv-log.txt"

	verbPtr, _ := syscall.UTF16PtrFromString(verb)
	exePtr, _ := syscall.UTF16PtrFromString(exe)
	cwdPtr, _ := syscall.UTF16PtrFromString(cwd)
	argPtr, _ := syscall.UTF16PtrFromString(args)

	var showCmd int32 = 1 //SW_NORMAL

	err := windows.ShellExecute(0, verbPtr, exePtr, argPtr, cwdPtr, showCmd)
	if err != nil {
		fmt.Println(err)
	}
}

func installSpiceAgent() {
	verb := "runas"
	exe := "virtio-win-guest-tools.exe"
	cwd := "D:\\"
	args := "/S"

	verbPtr, _ := syscall.UTF16PtrFromString(verb)
	exePtr, _ := syscall.UTF16PtrFromString(exe)
	cwdPtr, _ := syscall.UTF16PtrFromString(cwd)
	argPtr, _ := syscall.UTF16PtrFromString(args)

	var showCmd int32 = 1 //SW_NORMAL

	err := windows.ShellExecute(0, verbPtr, exePtr, argPtr, cwdPtr, showCmd)
	if err != nil {
		fmt.Println(err)
	}
}

func installGuestAgent() {
	verb := "runas"
	exe := "msiexec"
	cwd := "D:\\guest-agent"
	args := "/i \"qemu-ga-x86_64.msi\" /qn /l c:\\ga-log.txt"

	verbPtr, _ := syscall.UTF16PtrFromString(verb)
	exePtr, _ := syscall.UTF16PtrFromString(exe)
	cwdPtr, _ := syscall.UTF16PtrFromString(cwd)
	argPtr, _ := syscall.UTF16PtrFromString(args)

	var showCmd int32 = 1 //SW_NORMAL

	err := windows.ShellExecute(0, verbPtr, exePtr, argPtr, cwdPtr, showCmd)
	if err != nil {
		fmt.Println(err)
	}
}

func getLastLineWithSeek(filepath string) string {
	fileHandle, err := os.Open(filepath)

	if err != nil {
		return ""
	}
	defer fileHandle.Close()

	line := ""
	var cursor int64 = 0
	stat, _ := fileHandle.Stat()
	filesize := stat.Size()
	for {
		cursor -= 1
		fileHandle.Seek(cursor, io.SeekEnd)

		char := make([]byte, 1)
		fileHandle.Read(char)

		if cursor != -1 && (char[0] == 10 || char[0] == 13) { // stop if we find a line
			break
		}

		line = fmt.Sprintf("%s%s", string(char), line) // there is more efficient way

		if cursor == -filesize { // stop if we are at the begining
			break
		}
	}

	return line
}
