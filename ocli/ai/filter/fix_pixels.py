import numpy as np
def fix_pixels(image, bad_pixels):
    # 2d array of bad pixel indexes
    # log.error('NUMPY')
    bad_indices = np.nonzero(bad_pixels)
    # print("bad points %s ",bad_indices[0].size)
    mean = np.zeros(bad_indices[0].size, dtype=np.float32)
    mean_good = np.zeros(bad_indices[0].size, dtype=np.float32)
    norm = np.zeros(bad_indices[0].size, dtype=np.int32)
    shifts = ((-1, -1),
              (-1, 0),
              (-1, 1),
              (0, 1),
              (1, 1),
              (1, 0),
              (1, -1),
              (0, -1))
    # TODO - split by CPU cores

    for di, dj in shifts:
        # clip kernel shifts to image bounds
        I = np.clip(bad_indices[0] + di, 0, image.shape[0] - 1)
        J = np.clip(bad_indices[1] + dj, 0, image.shape[1] - 1)
        # J = _clip(bad_indices[1], dj, image.shape[1] - 1)
        # get pixel by kernel
        px = image[I, J]
        # is it bad
        bmask = bad_pixels[I, J]
        # dirty mean
        mean += px
        # mean by good pixels
        mean_good += np.where(bmask, 0, px)
        # count good pixels
        norm += (~bmask).astype(np.int32)
    mean /= len(shifts)
    mean_good /= np.maximum(1, norm)
    # print("fixing {} {}  with {} {}".format(bad_indices[0],bad_indices[1]),mean,mean_good,)
    image[bad_indices[0], bad_indices[1]] = np.where(norm <= len(shifts) / 2, mean, mean_good)