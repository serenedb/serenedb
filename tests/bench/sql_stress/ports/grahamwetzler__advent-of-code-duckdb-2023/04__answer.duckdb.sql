with recursive
  input as (
    select
      regexp_extract(column0, '(\d+)')::int as card
      , regexp_extract_all(column0[9:], '(\d+)')::int[] as card_numbers
      , regexp_extract_all(column1, '(\d+)')::int[] as winning_numbers
    from read_csv_auto('04/input.csv', sep='|', header=false)
  )

  , card_numbers as (
    select
      card
      , unnest(card_numbers) as card_number
    from input
  )

  , winning_numbers as (
    select
      card
      , unnest(winning_numbers) as winning_number
    from input
  )

  , winning as (
    select
      card
      , card_number
      , winning_number
      , card_number = winning_number as is_win
    from card_numbers
    join winning_numbers
    using (card)
  )

  , wins as (
    select
      card
      , sum(
          case
            when is_win then 1 else 0
          end
      )::int32 as win_count
    from winning
    group by all
  )

  , score as (
    select
      card
      , case
          when win_count = 1 then 1
          when win_count > 1 then 1 * 2^(win_count - 1)
      end::int as score
    from wins
    group by all
  )

  , cards_won as (
    select
      card
      , range(card + 1, card + win_count + 1) as cards_won
    from wins
  )

  , unnested_cards_won as (
    select
      card
      , unnest(cards_won) as won
    from cards_won
  )

  , u2 as (
    select
      cards_won.card
      , unnested_cards_won.won
    from cards_won
    left join unnested_cards_won
    using (card)
  )

  , recursive_check as (
    select won
    from unnested_cards_won

    union all

    select ucw.won
    from recursive_check
    join unnested_cards_won as ucw on
      recursive_check.won = ucw.card
  )

  , originals_plus_copies as (
    select card as won
    from score

    union all

    select won
    from recursive_check
  )

  , total_won as (
    select
      won
      , count(*) as count_won
    from originals_plus_copies
    group by all
    order by won
  )

select
  1 as part
  , sum(score) as answer
from score

union all

select
  2 as part
  , sum(count_won)
from total_won
