package info.bitrich.xchangestream.bitmex.dto;

import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

import static info.bitrich.xchangestream.bitmex.dto.BitmexLimitOrder.ASK_SIDE;
import static info.bitrich.xchangestream.bitmex.dto.BitmexLimitOrder.BID_SIDE;

/**
 * Created by Lukas Zaoralek on 13.11.17.
 */
public class BitmexOrderbook {
    private static final Logger LOG = LoggerFactory.getLogger(BitmexOrderbook.class);

    private SortedMap<BigDecimal, BitmexLimitOrder> asks;
    private SortedMap<BigDecimal, BitmexLimitOrder> bids;

    private Map<String, BigDecimal> askIds;
    private Map<String, BigDecimal> bidIds;

    public BitmexOrderbook() {
        this.askIds = new HashMap<>();
        this.bidIds = new HashMap<>();
        this.asks = Collections.synchronizedSortedMap(new TreeMap<>());
        this.bids = Collections.synchronizedSortedMap(new TreeMap<>(java.util.Collections.reverseOrder()));
    }

    public BitmexOrderbook(BitmexLimitOrder[] levels) {
        this();
        createFromLevels(levels);
    }

    public void createFromLevels(BitmexLimitOrder[] levels) {
        for (BitmexLimitOrder level : levels) {
            SortedMap<BigDecimal, BitmexLimitOrder> orderBookSide = level.getSide().equals(ASK_SIDE) ? asks : bids;
            Map<String, BigDecimal> orderBookSideIds = level.getSide().equals(ASK_SIDE) ? askIds : bidIds;
            orderBookSide.put(level.getPrice(), level);
            orderBookSideIds.put(level.getId(), level.getPrice());
        }
    }

    public void updateLevels(BitmexLimitOrder[] levels, String action) {
        for (BitmexLimitOrder level : levels) {
            updateLevel(level, action);
        }
    }

    public void updateLevel(BitmexLimitOrder level, String action) {
        SortedMap<BigDecimal, BitmexLimitOrder> orderBookSide = level.getSide().equals(ASK_SIDE) ? asks : bids;
        Map<String, BigDecimal> orderBookSideIds = level.getSide().equals(ASK_SIDE) ? askIds : bidIds;

        if (action.equals("insert")) {
            orderBookSide.put(level.getPrice(), level);
            orderBookSideIds.put(level.getId(), level.getPrice());
        } else if (action.equals("delete") || action.equals("update")) {
            boolean shouldDelete = action.equals("delete");
            String id = level.getId();
            BigDecimal price = orderBookSideIds.get(id);

            if (price != null) {
                orderBookSide.remove(price);
                orderBookSideIds.remove(id);

                if (!shouldDelete) {
                    BitmexLimitOrder modifiedLevel = new BitmexLimitOrder(level.getSymbol(), level.getId(), level.getSide(), price,
                            level.getSize()); // Original level doesn't have price! see bitmex doc
                    orderBookSide.put(price, modifiedLevel);
                    orderBookSideIds.put(id, price);
                }
            }
            else {
                LOG.warn("Could not update orderBookSide/orderBookSideIds as price was null, couldn't find id {}", id);
            }
        }
    }

    public BitmexLimitOrder[] getLevels(String side) {
        SortedMap<BigDecimal, BitmexLimitOrder> orderBookSide = side.equals(ASK_SIDE) ? asks : bids;
        return orderBookSide.values().toArray(new BitmexLimitOrder[orderBookSide.size()]);
    }

    public BitmexLimitOrder[] getAsks() {
        return getLevels(ASK_SIDE);
    }

    public BitmexLimitOrder[] getBids() {
        return getLevels(BID_SIDE);
    }

    public static List<LimitOrder> toLimitOrders(BitmexLimitOrder[] levels) {
        if (levels == null || levels.length == 0) return null;

        List<LimitOrder> limitOrders = new ArrayList<>(levels.length);
        for (BitmexLimitOrder level : levels) {
            if (level == null) {
                LOG.warn("Found null level on levels array!!");
                continue;
            }
            LimitOrder limitOrder = level.toLimitOrder();
            limitOrders.add(limitOrder);
        }

        return limitOrders;
    }

    public OrderBook toOrderbook() {
        List<LimitOrder> orderbookAsks = toLimitOrders(getAsks());
        List<LimitOrder> orderbookBids = toLimitOrders(getBids());
        return new OrderBook(null, orderbookAsks, orderbookBids);
    }
}
