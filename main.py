from pybaseball import playerid_lookup, statcast, statcast_batter, statcast_pitcher, batting_stats, pitching_stats
import statsapi
import datetime

if __name__ == '__main__':
    # statsapi
    date = datetime.datetime.now()
    form_date = date.strftime("%m/%d/%Y")
    sched = statsapi.schedule(start_date=form_date,end_date=form_date)
    # for g in sched:
    #     print(g)
    #     print('')
    
    # pybaseball (scraper with statcast)
    pydate = date.strftime("%Y-%m-%d")
    DC_id = playerid_lookup('carlson', 'dylan')
    # print(DC_id.key_mlbam[0])
    DC_stats = statcast_batter('2022-05-01', pydate, DC_id.key_mlbam[0])
    # print(DC_stats.head())
    
    all_hitters = batting_stats(2019,2022)
    print(all_hitters.head())
    all_pitchers = pitching_stats(2019,2022)
    print(all_pitchers.head())
