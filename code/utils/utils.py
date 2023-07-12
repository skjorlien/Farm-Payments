from PIL import Image 
import glob, os


def gifify(images, fname):
    frames = [Image.open(image) for image in images]
    frame_one = frames[0]
    frame_one.save(fname, format = 'GIF', append_images = frames, 
                   save_all=True, duration=1000, loop=0) 
