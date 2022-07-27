import cv2
import pytesseract  # requires installation of Tesseract software and the placement in PATH
import numpy as np
import re
import os
import csv
# import pandas as pd
import argparse
import dateparser
# from datetime import datetime, timezone, timedelta


def get_grayscale(image):
    """
    Returns greyscale conversion.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)


# noise removal
def remove_noise(image):
    """
    Applies meddianblue to remove noise.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    return cv2.medianBlur(image, 5)


# thresholding
def thresholding(image):
    """
    Create a black and white image with automatic thresholding.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    return cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]


# dilation
def dilate(image):
    """
    Used to diminish the features of an image.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    kernel = np.ones((5, 5), np.uint8)
    return cv2.dilate(image, kernel, iterations=1)


# erosion
def erode(image):
    """
    Erodes away the boundaries of the foreground object.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    kernel = np.ones((5, 5), np.uint8)
    return cv2.erode(image, kernel, iterations=1)


def opening(image):
    """
    opening - erosion followed by dilation

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    kernel = np.ones((5, 5), np.uint8)
    return cv2.morphologyEx(image, cv2.MORPH_OPEN, kernel)


def canny(image):
    """
    Canny edge detection.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    return cv2.Canny(image, 100, 200)


def deskew(image):
    """
    Skew correction.

    :param image: cv2.imread object.
    :return: cv2.imread object.
    """
    coords = np.column_stack(np.where(image > 0))
    angle = cv2.minAreaRect(coords)[-1]
    if angle < -45:
        angle = -(90 + angle)
    else:
        angle = -angle
    (h, w) = image.shape[:2]
    center = (w // 2, h // 2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)
    rotated = cv2.warpAffine(image, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
    return rotated


def match_template(image, template):
    """
    Find image within another image.

    :param image: cv2.imread object.
    :param template: cv2.imread object.
    :return: cv2.imread object.
    """
    return cv2.matchTemplate(image, template, cv2.TM_CCOEFF_NORMED)


def convert_date(dt_str):
    """
    Converts a date time read from a phenocam stamp on an image and converts it to an iso formatted string.

    :param dt_str: character string. Datetime read from a phenocam stamp on an image.
    :return: character string. Iso formatted datetime string.
    """
    pattern = re.compile(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+'
                         r'([0-3]\d)\s+([12][09]\d{2})\s+([0-2]\d):?([0-5]\d):?([0-5]\d)\s+([A-Z]+)')
    grps = re.findall(pattern, dt_str)
    if grps:
        dt = grps[0]
        dt_new = ' '.join((dt[1], dt[2], dt[3], ':'.join((dt[4], dt[5], dt[6])), dt[7]))
    else:
        print("\tRegex failed on", repr(dt_str))
        return None
    dtp = dateparser.parse(dt_new)
    if dtp is None:
        print("\tCouldn't parse", repr(dt_new))
        return None
    else:
        return dtp.isoformat(sep=' ')


def read_photos(dirpath):
    """
    Reads phenocam photos and tries to read the image stamp information that the phenocam impregnates in the image.

    :param dirpath: character string. The path to scan for phenocam photos.
    :return: A list containing ocr data extracted from phenocam photos.
    """
    print('Scanning images...')
    flist = []
    custom_config = r'--oem 3 --psm 6'
    for root, dirs, files in os.walk(dirpath):
        for f in files:
            dt_list = []
            camera_name = None
            camera_type = None
            dt_iso = None
            exposure = None
            path = os.path.join(root, f)
            if os.path.splitext(f)[1] in ['.jpg', '.jpeg']:
                print(path)
                try:
                    image = cv2.imread(path)
                except cv2.error:
                    print("\tCan't open file.")
                else:
                    if image is not None:
                        cropped = image[0:int(image.shape[0] * 0.07), 0:image.shape[1]]
                        try:
                            gray = get_grayscale(cropped)
                        except cv2.error:
                            print("\tGrayscale conversion failed.")
                        else:
                            try:
                                thresh = thresholding(gray)
                            except cv2.error:
                                print("\tThresholding failed.", path)
                            else:
                                try:
                                    ocr = pytesseract.image_to_string(thresh, config=custom_config)
                                except pytesseract.TesseractError:
                                    print("\tOCR failed.")
                                else:
                                    dt_list = re.split(r'(?:\s*\-\s*|\n)', ocr)
                                    print('\t', repr(ocr))
                    if len(dt_list) >= 4:
                        camera_name = re.sub(r"^\[*(.+?)\]*$", r'\1', dt_list[0])  # removes beg. and end brackets
                        camera_type = dt_list[1]
                        dt_iso = convert_date(dt_str=ocr)
                        exp_str = re.findall(r"[Ee][Xx][A-Za-z]+[:;]\s*(\d+)", ocr)
                        if exp_str:
                            try:
                                exposure = int(exp_str[0])
                            except ValueError:
                                print('\tCould not convert exposure,', exp_str)
                    else:
                        print('\tCould not split ocr text')
                    rel_path = re.sub(r'^[\/\\]', '', path.replace(dirpath, ''))
                    row = {'path': rel_path, 'camera_name': camera_name, 'camera_type': camera_type, 'dt_iso': dt_iso,
                           'exposure': exposure, 'ocr': ocr}
                    flist.append(row)
    return flist


def write_csv(scandict, outfile):
    """
    Writes the results of the read_photos function to a delimited file.

    :param scandict: A list created by the read_photos function.
    :param outfile: character string. Path to which to save the scan results.
    """
    with open(outfile, 'w', newline='') as csvfile:
        fieldnames = ['path', 'camera_name', 'camera_type', 'dt_iso', 'exposure', 'ocr']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for r in scandict:
            writer.writerow(r)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder with PhenoCam style dates and '
                                                 'exposure values printed on the photo and export them to a delimited '
                                                 'file.')
    parser.add_argument('scanpath', help='path to recursively scan for image files')
    parser.add_argument('outfile', help='the path to store the output csv.')
    args = parser.parse_args()
    scandict = read_photos(dirpath=args.scanpath)
    write_csv(scandict=scandict, outfile=args.outfile)
    print('Script finished.')
