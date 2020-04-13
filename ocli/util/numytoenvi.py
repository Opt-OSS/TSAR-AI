#!/usr/bin/env python3
#%%
import numpy as np
from pathlib import Path
from matplotlib import pyplot as plt
dir = '/e/DEMO-TMP/bc-b-1/623A_CEDA_IW3_4_4_groningen_vlz_cluster'

#%%
#
# t = np.load(Path(dir,'zone_tnsr.npy'))
# t2 = np.fromfile(Path(dir,'zone_prob_pred.npy.img'))
# t2 = t2.reshape(*t.shape[:2],16)
# #%%
# fig = plt.figure()
# plt.gray()  # show the filtered result in grayscale
# ax1 = fig.add_subplot(121)  # left side
# ax2 = fig.add_subplot(122)  # right side
# ax1.imshow(t[...,5])
# # ax2.imshow(t2[100:200,250:350,8])
# ax2.imshow(t2[...,8])
# plt.show()



#%%
if __name__ =='__main__':
    t = np.load(Path(dir,'zone_tnsr.npy'))
    t.tofile(Path(dir,'zone_tnsr.img'))
