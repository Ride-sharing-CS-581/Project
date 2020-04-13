import matplotlib.pyplot as plt
import numpy as np

def bgraph1(p5, p10, Tp5, Tp10):
    labels = ['5', '10']
    F5 = [p5, Tp5]
    F10 = [p10, Tp10]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width / 2, F5, width, label='From LGA')
    rects2 = ax.bar(x + width / 2, F10, width, label='To LGA')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Average distance saved per pool (miles)')
    ax.set_xlabel('Pool window')
    ax.set_title('Graph one')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    plt.show()

def bgraph3(p5, p10, Tp5, Tp10):
    labels = ['5', '10']
    F5 = [p5, Tp5]
    F10 = [p10, Tp10]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width / 2, F5, width, label='From LGA')
    rects2 = ax.bar(x + width / 2, F10, width, label='To LGA')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Average Computation time per pool(seconds)')
    ax.set_xlabel('Pool window')
    ax.set_title('Graph Three')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    plt.show()



def bgraph2(p5, p10, Tp5, Tp10):

    labels = ['5', '10']
    F5 = [p5, Tp5]
    F10 = [p10, Tp10]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width / 2, F5, width, label='From LGA')
    rects2 = ax.bar(x + width / 2, F10, width, label='To LGA')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Average number of trips saved per pool')
    ax.set_xlabel('Pool window')
    ax.set_title('Graph Two')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    plt.show()

#bgraph1(12, 15, 10, 16)
#bgraph2(12, 15, 10, 16)
#bgraph3(12, 15, 10, 16)
