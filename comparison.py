from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import data.data as dm
import argparse
import seaborn
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser()
parser.add_argument("--category", "-c", type=str,
                    help="Property to focus on (cast_member, director, genre)",
                    choices=['genre', 'cast_member', 'director'])
parser.add_argument("--score", "-s", type=str,
                    help="Score to focus on (critic_percent, return)",
                    choices=['return', 'critic_percent', 'critic_average',
                             'audience_percent', 'audience_average'])
parser.add_argument("--movies", "-m", type=int,
                    help="Minimum number of movies of each property",
                    default=40)
parser.add_argument("--influencers", "-i", type=int,
                    help="Number of influential people/genres",
                    default=25)
parser.add_argument("--year", "-y", type=int, nargs='+',
                    help="Bound the publication dates by the years",
                    default=None)

args = parser.parse_args()

filename = '{}-{}-{}-{}'.format(args.category, args.score, args.movies,
                                args.influencers)

if args.year:
    filename += "-{}-{}".format(args.year[0], args.year[1])

ALPHA = 0.05


def main():
    seaborn.set()
    data = dm.get_movie_data(filename, args)

    print("DEGUG: TukeyHSD")
    data_pivoted = data.pivot(columns=args.category)[args.score]
    new_data = [data_pivoted[col].dropna() for col in data_pivoted]
    anova = stats.f_oneway(*new_data)
    print("P Value from f_oneway of the {} {}s is {}".format(
        args.influencers,
        args.category.replace("_", " "),
        anova.pvalue))

    if anova.pvalue <= ALPHA:
        print("There is a difference between means, proceed to Tukey's HSD")
        posthoc = pairwise_tukeyhsd(
            data[args.score], data[args.category],
            alpha=ALPHA)

        print(data.groupby(args.category).agg({
            args.score: 'mean'}).sort_values(args.score, ascending=False))

        title_score = args.score.title().replace("_", " ")
        if args.score == "return":
            xlabel = "Percent Return (box office/cost)"
        else:
            xlabel = "Rotten Tomatoes' {}".format(
                title_score)
        ylabel = args.category.title().replace("_", " ") + 's'

        title = "{}' {} Comparison".format(
            ylabel, title_score
        )
        if args.year:
            title += " from {} to {}".format(args.year[0], args.year[1])

        ax = plt.axes()
        plt.subplots_adjust(left=.20)
        ax.yaxis.label.set_size(20)
        ax.xaxis.label.set_size(20)
        for tick in ax.yaxis.get_major_ticks():
            tick.label.set_fontsize(16)
            tick.label.set_rotation(30)

        for tick in ax.xaxis.get_major_ticks():
            tick.label.set_fontsize(14)

        fig = posthoc.plot_simultaneous(
            xlabel=xlabel,
            ylabel=ylabel,
            ax=ax,
            figsize=(16, 8),
            )
        fig.suptitle(title)
        fig.savefig("figures/" + filename)
    else:
        print("Can't confirm there is a difference between means")

    print("Done!")


if __name__ == '__main__':
    main()
