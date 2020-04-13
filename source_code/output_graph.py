import matplotlib.pyplot as plt


def graph1(p5, p10, Tp5, Tp10):
    # line 1 points
    x1 = ["5", "10"]
    y1 = [p5, p10]
    # plotting the line 1 points
    plt.plot(x1, y1, label="From LGA")

    # line 2 points
    x2 = ["5", "10"]
    y2 = [Tp5, Tp10]
    # plotting the line 2 points
    plt.plot(x2, y2, label="To LGA")

    # naming the x axis
    plt.xlabel('Pool window (min)')
    # naming the y axis
    plt.ylabel('Average distance saved per pool (miles)')
    # giving a title to my graph
    plt.title('Average distance saved per pool as a % of the total distance of individual (unshared) trips')

    # show a legend on the plot
    plt.legend()

    # function to show the plot
    plt.show()


def graph3(p5, p10, Tp5, Tp10):
    # line 1 points
    x1 = ["5", "10"]
    y1 = [p5, p10]
    # plotting the line 1 points
    plt.plot(x1, y1, label="From LGA")

    # line 2 points
    x2 = ["5", "10"]
    y2 = [Tp5, Tp10]
    # plotting the line 2 points
    plt.plot(x2, y2, label="To LGA")

    # naming the x axis
    plt.xlabel('Pool window (min)')
    # naming the y axis
    plt.ylabel('Average Computation time per pool (seconds)')
    # giving a title to my graph
    plt.title('Average computation time per pool')

    # show a legend on the plot
    plt.legend()

    # function to show the plot
    plt.show()


def graph2(p5, p10, Tp5, Tp10):
    # line 1 points
    x1 = ["5", "10"]
    y1 = [p5, p10]
    # plotting the line 1 points
    plt.plot(x1, y1, label="From LGA")

    # line 2 points
    x2 = ["5", "10"]
    y2 = [Tp5, Tp10]
    # plotting the line 2 points
    plt.plot(x2, y2, label="To LGA")

    # naming the x axis
    plt.xlabel('Pool window (min)')
    # naming the y axis
    plt.ylabel('Average number of trips saved per pool')
    # giving a title to my graph
    plt.title('Average number of trips saved per pool as a % of the number of individual trips')

    # show a legend on the plot
    plt.legend()

    # function to show the plot
    plt.show()


# From LaG 5 min pool, From Lag 10 min pool, To Lag 5 min pool, To Lag 10 min pool
# graph1(15.074967, 37.93, 30, 67.98)
# graph2(12, 15, 10, 16)
# graph3(12, 15, 10, 16)
