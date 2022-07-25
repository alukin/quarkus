import {doPost, errorPopUp} from "../web/web.js";
import {createTableItem} from "../util/contentManagement.js";
import {toggleSpinner} from "../util/spinner.js";

export default class AccessControlListPage{
    constructor(containerId) {
        this.containerId = containerId;
        Object.getOwnPropertyNames(AccessControlListPage.prototype).forEach((key) => {
            if (key !== 'constructor') {
                this[key] = this[key].bind(this);
            }
        });
    }


    open() {
        const req = {
            action: "getAclInfo"
        };
        doPost(req, (data) => {
            let that = this;
            setTimeout(function () {
                that.updateInfo(data);
                toggleSpinner(that.containerId);
            }, 2000);
        }, data => {
            errorPopUp("Error getting Kafka ACL info: ", data);
        });      
    }

    updateInfo(data) {
        $('#cluster-id').html(data.clusterId);
        $('#cluster-controller').html(data.broker);
        $('#cluster-acl').html(data.aclOperations);

        const acls = data.entires;
        let aclTable = $('#acl-table tbody');
        aclTable.empty();
        for (let i = 0; i < acls.length; i++) {
            const e = acls[i];
            let tableRow = $("<tr/>");
            tableRow.append(createTableItem(e.operation));
            tableRow.append(createTableItem(e.prinipal));
            tableRow.append(createTableItem(e.perm));
            tableRow.append(createTableItem(e.pattern));
            aclTable.append(tableRow);
        }
    }
}