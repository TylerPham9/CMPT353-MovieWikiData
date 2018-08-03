import matplotlib.pyplot as plt
import data.data as dm
import argparse
import seaborn
import math

parser = argparse.ArgumentParser()

parser.add_argument("--category", "-c", type=str,
                    help="Property to focus on (cast_member, director, genre)",
                    choices=['genre', 'cast_member', 'director'])
parser.add_argument("--score", "-s", type=str,
                    help="Score to focus on (critic_percent, return)",
                    choices=['return', 'critic_percent', 'critic_average',
                             'audience_percent', 'audience_average'])
parser.add_argument("--movies", "-mo", type=int,
                    help="Minimum number of movies of each property",
                    default=40)
parser.add_argument("--influencers", "-i", type=int,
                    help="Number of influential people/genres",
                    default=25)

args = parser.parse_args()

filename = '{}-{}-{}-{}'.format(args.category, args.score, args.movies,
                                args.influencers)


def get_decade(year):
    """
    Rounds the year to the decade
    :param year: int, year of publication
    :return: int, decade of movie
    """
    return int(math.floor(year / 10.0)) * 10


def main():
    seaborn.set()
    data = dm.get_movie_data(filename, args)
    data['year'] = data['publication_date'].apply(dm.get_year)
    data['decade'] = data['year'].apply(get_decade)
    decade_avg = data.groupby(
        [args.category, 'decade']).agg(
        {args.score: 'mean'})

    categories = data[args.category].unique()
    plt.figure(figsize=(16, 8))
    for category in categories:
        data = decade_avg.loc[category]
        plt.plot(data.index, data.values, 'o-')
    plt.legend(categories, prop={'size': 14})
    plt.xlabel("Decades", fontsize=20)
    title_score = args.score.title().replace("_", " ")
    plt.ylabel("Average {}".format(title_score), fontsize=20)
    plt.title(
        "Average {} of {}s".format(
            title_score, args.category.title().replace("_", " ")),
        fontsize=20,
    )
    # plt.show()
    plt.savefig("figures/yearly-" + filename)
    print("Yearly Done")


if __name__ == "__main__":
    main()