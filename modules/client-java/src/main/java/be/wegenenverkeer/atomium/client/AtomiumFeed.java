package be.wegenenverkeer.atomium.client;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Emitter;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.BiConsumer;
import io.reactivex.rxjava3.functions.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AtomiumFeed<E> {
    private static String FIRST_PAGE = "/";
    public static final FeedPosition HEAD_PAGE_FEED_POSITION = FeedPositions.of(AtomiumFeed.FIRST_PAGE);
    private final static Logger logger = LoggerFactory.getLogger(AtomiumFeed.class);
    private final PageFetcher<E> pageFetcher;

    public AtomiumFeed(PageFetcher<E> pageFetcher) {
        this.pageFetcher = pageFetcher;
    }

    private int retryCount = 0;
    private AtomicReference<CachedFeedPage<E>> generatorState = new AtomicReference<>();

    public Flowable<FeedEntry<E>> fetchEntries(FeedPositionStrategy feedPositionStrategy) {
        return fetchHeadPage().toFlowable().concatMap(headPage -> fetchEntries(headPage, feedPositionStrategy));
    }

    private Flowable<FeedEntry<E>> fetchEntries(CachedFeedPage<E> initialPage, FeedPositionStrategy feedPositionStrategy) {
        generatorState.set(initialPage);

        return Flowable.generate(getNextPage(feedPositionStrategy))
                .concatMap(pageRef -> pageRef
                        .toFlowable()
                        .doOnNext(nextPage -> logger.debug("Parsing page {}", nextPage.getSelfHref()))
                        .map(nextPage -> ParsedFeedPage.parse(nextPage, FeedPositions.of(nextPage.getSelfHref())))// TODO fix, we should return a tuple of page and feed position
                        .flatMap(parsedPage -> Flowable.fromIterable(parsedPage.getEntries()))
                        .doOnNext(eFeedEntry -> logger.debug("Emitting feed entry {} on page {}", eFeedEntry.getEntry().getId(), eFeedEntry.getSelfHref())
                        ), 1);
    }

    private Consumer<Emitter<Single<CachedFeedPage<E>>>> getNextPage(FeedPositionStrategy feedPositionStrategy) {
        return (feedEntryEmitter) -> {
            logger.debug("Generator fired, calculating next page..."); // dit gebeurt niet in volgorde...
            Single<CachedFeedPage<E>> nextPage = feedPositionStrategy.getNextFeedPosition(generatorState.get())
                    .doOnSuccess(nextFeedPos -> logger.debug("Next page will be {}", nextFeedPos.getPageUrl()))
                    .concatMap(this::fetchPage)
                    .doOnSuccess(fetchedPage -> logger.debug("Page {} fetched!", fetchedPage.getSelfHref()))
                    .doOnSuccess(fetchedPage -> generatorState.set(fetchedPage));
            logger.debug("Emitting value..."); // dit gebeurt niet in volgorde...
            feedEntryEmitter.onNext(nextPage);
        };
    }

    private Single<CachedFeedPage<E>> fetchHeadPage() {
        return fetchPage(HEAD_PAGE_FEED_POSITION);
    }

    private Single<CachedFeedPage<E>> fetchPage(FeedPosition feedPosition) {
        return pageFetcher.fetch(feedPosition.getPageUrl(), Optional.empty())
                .retryWhen(throwableFlowable -> throwableFlowable
                        .flatMap(this::applyRetryStrategy)
                        .flatMap(delay -> Flowable.just("ignored").delay(delay.longValue(), TimeUnit.MILLISECONDS))
                )
                .doAfterSuccess(page -> this.retryCount = 0);
    }

    private Flowable<Long> applyRetryStrategy(Throwable throwable) {
        try {
            return Flowable.just(pageFetcher.getRetryStrategy().apply(++this.retryCount, throwable));
        } catch(Throwable e) {
            return Flowable.error(e);
        }
    }
}
